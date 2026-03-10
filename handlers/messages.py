"""
Обработка сообщений: маршрутизация, доставка, удаление, редактирование, реакции.
"""
import logging
import uuid
from datetime import datetime
from typing import Optional

from fastapi import WebSocket

from server_metrics import track_message_sent

logger = logging.getLogger("DDChatRelay")


def _now_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


class MessageHandler:
    def __init__(self, connection_manager, offline_queue, push_service, rate_limiter):
        self._cm   = connection_manager
        self._oq   = offline_queue
        self._push = push_service
        self._rate  = rate_limiter

    async def _deliver_or_store(self, target_uid: str, payload: dict,
                                 push_type: str, from_uid: str,
                                 redis_client, group_id: str = None) -> bool:
        """Доставляет онлайн или сохраняет в офлайн-очередь + push."""
        delivered = await self._cm.deliver_to_uid(target_uid, payload)

        if not delivered:
            await self._oq.store(redis_client, target_uid, payload)
            await self._push.send_push(redis_client, target_uid, from_uid, push_type, group_id)

        track_message_sent(delivered)
        return delivered

    async def _route_message(self, target_uid: str, payload: dict,
                              push_type: str, from_uid: str, redis_client) -> bool:
        """Маршрутизирует: группа → fan-out, личное → direct."""
        if target_uid.startswith("g_"):
            if redis_client:
                members = await redis_client.smembers(f"group:{target_uid}")
                delivered_any = False
                for member in members:
                    if member != from_uid:
                        deliv = await self._deliver_or_store(
                            member, payload, push_type, from_uid, redis_client, group_id=target_uid
                        )
                        if deliv:
                            delivered_any = True
                return delivered_any
            return False
        else:
            return await self._deliver_or_store(target_uid, payload, push_type, from_uid, redis_client)

    async def handle_message(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        """Обработка type=message."""
        if not await self._rate.check(my_uid, redis_client):
            await self._cm.send_to(websocket, {
                "type": "error",
                "message": "Rate limit exceeded. Please slow down.",
            })
            return

        target_uid = data.get("target_uid")
        message_id = data.get("id")
        if not target_uid or not message_id:
            return

        # Group admin restriction
        if redis_client and str(target_uid).startswith("g_"):
            settings = await redis_client.hgetall(f"group_settings:{target_uid}")
            if settings.get("only_admins_post") == "1":
                admins = await redis_client.smembers(f"group_admins:{target_uid}")
                if my_uid not in admins:
                    await self._cm.send_to(websocket, {"type": "error", "message": "Only admins can post in this group"})
                    return

        raw_payload = {
            "type":           "message",
            "from_uid":       my_uid,
            "target_uid":     target_uid,
            "id":             message_id,
            "encrypted_text": data.get("encrypted_text"),
            "signature":      data.get("signature"),
            "time":           _now_ms(),
            "replyToId":      data.get("replyToId"),
            "messageType":    data.get("messageType", "text"),
            "mediaData":      data.get("mediaData"),
            "fileName":       data.get("fileName"),
            "fileSize":       data.get("fileSize"),
            "mimeType":       data.get("mimeType"),
            "group_id":       data.get("group_id"),
            "forwarded_from": data.get("forwarded_from"),
            # Disappearing messages support
            "ttl_seconds":    data.get("ttl_seconds"),
        }
        payload = {k: v for k, v in raw_payload.items() if v is not None}

        delivered = await self._route_message(target_uid, payload, "new_message", my_uid, redis_client)
        await self._cm.send_to(websocket, {"type": "server_ack", "id": message_id, "delivered_online": delivered})

    async def handle_delete(self, my_uid: str, data: dict, redis_client):
        target_uid = data.get("target_uid")
        message_id = data.get("message_id")
        if target_uid and message_id:
            payload = {
                "type": "message_deleted", "target_uid": target_uid,
                "from_uid": my_uid, "message_id": message_id, "time": _now_ms(),
            }
            await self._route_message(target_uid, payload, "message_deleted", my_uid, redis_client)

    async def handle_edit(self, my_uid: str, data: dict, redis_client):
        target_uid = data.get("target_uid")
        message_id = data.get("message_id")
        if target_uid and message_id:
            payload = {
                "type":               "message_edited",
                "target_uid":         target_uid,
                "from_uid":           my_uid,
                "message_id":         message_id,
                "new_encrypted_text": data.get("new_encrypted_text"),
                "new_signature":      data.get("new_signature"),
                "time":               _now_ms(),
            }
            await self._route_message(target_uid, payload, "message_edited", my_uid, redis_client)

    async def handle_reaction(self, my_uid: str, data: dict, redis_client):
        target_uid = data.get("target_uid")
        message_id = data.get("message_id")
        if target_uid and message_id:
            payload = {
                "type":       "message_reaction",
                "target_uid": target_uid,
                "from_uid":   my_uid,
                "message_id": message_id,
                "emoji":      data.get("emoji"),
                "action":     data.get("action"),
                "time":       _now_ms(),
            }
            await self._route_message(target_uid, payload, "message_reaction", my_uid, redis_client)

    async def handle_receipt(self, my_uid: str, msg_type: str, data: dict):
        target_uid = data.get("target_uid")
        if target_uid:
            ws = self._cm.get_ws(target_uid)
            if ws:
                await self._cm.send_to(ws, {
                    "type":       msg_type,
                    "from_uid":   my_uid,
                    "target_uid": target_uid,
                    "message_id": data.get("message_id"),
                    "time":       _now_ms(),
                })

    async def handle_typing(self, my_uid: str, data: dict):
        target_uid = data.get("target_uid")
        if target_uid:
            # Для групп — typing рассылается всем участникам
            if target_uid.startswith("g_"):
                # В будущем: можно добавить fan-out для групп
                pass
            ws = self._cm.get_ws(target_uid)
            if ws:
                await self._cm.send_to(ws, {
                    "type":       "typing_indicator",
                    "from_uid":   my_uid,
                    "target_uid": target_uid,
                    "typing":     data.get("typing", False),
                })
