"""
Каналы: создание, подписка, поиск (с пагинацией), публикация.
"""
import json
import logging
import uuid
from datetime import datetime

from fastapi import WebSocket

logger = logging.getLogger("DDChatRelay")


def _now_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


# Лимит результатов поиска — защита от O(N) при тысячах каналов
SEARCH_PAGE_SIZE = 50


class ChannelHandler:
    def __init__(self, connection_manager):
        self._cm = connection_manager

    async def handle_create(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        channel_id   = data.get("channel_id") or f"ch_{uuid.uuid4().hex[:8]}"
        channel_name = data.get("channel_name", "").strip()
        description  = data.get("description", "")
        if not redis_client or not channel_name:
            return

        meta = {
            "name":        channel_name,
            "owner_uid":   my_uid,
            "description": description,
            "created_at":  str(_now_ms()),
        }
        await redis_client.hset(f"channel:{channel_id}", mapping=meta)
        await redis_client.sadd(f"channel_subs:{channel_id}", my_uid)
        await redis_client.zadd("channels_index", {channel_id: _now_ms()})
        await self._cm.send_to(websocket, {
            "type":         "channel_created",
            "channel_id":   channel_id,
            "channel_name": channel_name,
        })
        logger.info(f"📺 Channel created: {channel_id} ({channel_name}) by {my_uid}")

    async def handle_search(self, websocket: WebSocket, data: dict, redis_client):
        """Поиск каналов с пагинацией — O(page_size) вместо O(N)."""
        query  = str(data.get("query", "")).lower().strip()
        offset = int(data.get("offset", 0))
        limit  = min(int(data.get("limit", SEARCH_PAGE_SIZE)), SEARCH_PAGE_SIZE)

        if not redis_client:
            return

        # Используем ZSCAN для итерации (эффективнее zrange 0 -1)
        results = []
        cursor  = 0
        scanned = 0

        while len(results) < limit:
            cursor, channel_ids = await redis_client.zscan("channels_index", cursor=cursor, count=100)
            for cid, _ in channel_ids:
                meta = await redis_client.hgetall(f"channel:{cid}")
                if not meta:
                    continue
                if query and query not in meta.get("name", "").lower():
                    continue
                scanned += 1
                if scanned <= offset:
                    continue
                sub_count = await redis_client.scard(f"channel_subs:{cid}")
                results.append({
                    "channel_id":       cid,
                    "channel_name":     meta.get("name"),
                    "description":      meta.get("description", ""),
                    "owner_uid":        meta.get("owner_uid"),
                    "subscriber_count": sub_count,
                })
                if len(results) >= limit:
                    break
            if cursor == 0:
                break

        await self._cm.send_to(websocket, {
            "type":       "channel_search_results",
            "results":    results,
            "has_more":   cursor != 0 or scanned > offset + limit,
            "next_offset": offset + len(results),
        })

    async def handle_join(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        channel_id = data.get("channel_id")
        if not redis_client or not channel_id:
            return

        exists = await redis_client.exists(f"channel:{channel_id}")
        if not exists:
            await self._cm.send_to(websocket, {"type": "error", "message": "Channel not found"})
        else:
            await redis_client.sadd(f"channel_subs:{channel_id}", my_uid)
            meta = await redis_client.hgetall(f"channel:{channel_id}")
            await self._cm.send_to(websocket, {
                "type":         "channel_joined",
                "channel_id":   channel_id,
                "channel_name": meta.get("name", channel_id),
            })
            logger.info(f"📺 {my_uid} joined channel {channel_id}")

    async def handle_leave(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        channel_id = data.get("channel_id")
        if not redis_client or not channel_id:
            return

        await redis_client.srem(f"channel_subs:{channel_id}", my_uid)
        await self._cm.send_to(websocket, {"type": "channel_left", "channel_id": channel_id})
        logger.info(f"📺 {my_uid} left channel {channel_id}")

    async def handle_message(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        channel_id = data.get("channel_id")
        text       = data.get("text", "").strip()
        message_id = data.get("id") or uuid.uuid4().hex
        if not redis_client or not channel_id or not text:
            return

        meta = await redis_client.hgetall(f"channel:{channel_id}")
        if not meta:
            await self._cm.send_to(websocket, {"type": "error", "message": "Channel not found"})
            return

        if meta.get("owner_uid") != my_uid:
            await self._cm.send_to(websocket, {"type": "error", "message": "Only the channel owner can post"})
            return

        msg_payload = {
            "type":       "channel_message",
            "channel_id": channel_id,
            "from_uid":   my_uid,
            "id":         message_id,
            "text":       text,
            "time":       _now_ms(),
        }
        await redis_client.lpush(f"channel_history:{channel_id}", json.dumps(msg_payload))
        await redis_client.ltrim(f"channel_history:{channel_id}", 0, 499)

        subscribers = await redis_client.smembers(f"channel_subs:{channel_id}")
        for sub_uid in subscribers:
            if sub_uid != my_uid:
                ws = self._cm.get_ws(sub_uid)
                if ws:
                    await self._cm.send_to(ws, msg_payload)
        await self._cm.send_to(websocket, {"type": "server_ack", "id": message_id, "delivered_online": True})
        logger.info(f"📺 Channel message in {channel_id} from {my_uid}")

    async def handle_delete(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        """Удалить канал — только владелец."""
        channel_id = data.get("channel_id")
        if not redis_client or not channel_id:
            return

        meta = await redis_client.hgetall(f"channel:{channel_id}")
        if not meta:
            await self._cm.send_to(websocket, {"type": "error", "message": "Channel not found"})
            return
        if meta.get("owner_uid") != my_uid:
            await self._cm.send_to(websocket, {"type": "error", "message": "Only owner can delete"})
            return

        # Уведомляем подписчиков
        subscribers = await redis_client.smembers(f"channel_subs:{channel_id}")
        delete_msg = {"type": "channel_deleted", "channel_id": channel_id, "time": _now_ms()}
        for sub_uid in subscribers:
            ws = self._cm.get_ws(sub_uid)
            if ws:
                await self._cm.send_to(ws, delete_msg)

        # Удаляем данные
        await redis_client.delete(f"channel:{channel_id}")
        await redis_client.delete(f"channel_subs:{channel_id}")
        await redis_client.delete(f"channel_history:{channel_id}")
        await redis_client.zrem("channels_index", channel_id)
        logger.info(f"🗑️ Channel {channel_id} deleted by {my_uid}")
