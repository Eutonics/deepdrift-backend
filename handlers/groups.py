"""
Группы: создание, управление участниками, ключи, настройки.
Добавлено: delete_group, demote_admin.
"""
import json
import logging
import uuid
from datetime import datetime
from typing import Optional

from fastapi import WebSocket

from config import GROUP_KEY_TTL

logger = logging.getLogger("DDChatRelay")


def _now_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


class GroupHandler:
    def __init__(self, connection_manager, offline_queue, push_service):
        self._cm   = connection_manager
        self._oq   = offline_queue
        self._push = push_service

    async def _route_to_members(self, group_id: str, payload: dict,
                                 from_uid: str, redis_client, push_type: str = "group_update"):
        """Рассылает payload всем участникам группы (кроме from_uid)."""
        if not redis_client:
            return
        members = await redis_client.smembers(f"group:{group_id}")
        for member in members:
            if member == from_uid:
                continue
            delivered = await self._cm.deliver_to_uid(member, payload)
            if not delivered:
                await self._oq.store(redis_client, member, payload)
                await self._push.send_push(redis_client, member, from_uid, push_type, group_id)

    async def handle_create(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        group_id   = data.get("group_id")
        members    = data.get("members", [])
        group_name = data.get("group_name", group_id)
        if not redis_client or not group_id or not members:
            return

        if my_uid not in members:
            members.append(my_uid)
        await redis_client.sadd(f"group:{group_id}", *members)
        await redis_client.set(f"group_name:{group_id}", group_name)
        await redis_client.sadd(f"group_admins:{group_id}", my_uid)
        await self._cm.send_to(websocket, {"type": "group_created", "group_id": group_id})

        invite = {
            "type":        "group_invited",
            "group_id":    group_id,
            "group_name":  group_name,
            "creator_uid": my_uid,
            "from_uid":    my_uid,
            "members":     members,
        }
        await self._route_to_members(group_id, invite, my_uid, redis_client, "group_invited")

    async def handle_leave(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        group_id = data.get("group_id")
        if not redis_client or not group_id:
            return

        await redis_client.srem(f"group:{group_id}", my_uid)
        await redis_client.delete(f"group_key:{group_id}:{my_uid}")
        await redis_client.srem(f"group_admins:{group_id}", my_uid)

        leave_msg = {
            "type":     "group_member_left",
            "group_id": group_id,
            "uid":      my_uid,
            "time":     _now_ms(),
        }
        members = await redis_client.smembers(f"group:{group_id}")
        for member in members:
            if member != my_uid:
                ws = self._cm.get_ws(member)
                if ws:
                    await self._cm.send_to(ws, leave_msg)
        logger.info(f"👋 {my_uid} left group {group_id}")

    async def handle_kick(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        group_id   = data.get("group_id")
        target_uid = data.get("target_uid")
        if not redis_client or not group_id or not target_uid:
            return

        admins = await redis_client.smembers(f"group_admins:{group_id}")
        if my_uid not in admins:
            await self._cm.send_to(websocket, {"type": "error", "message": "Not an admin"})
            return

        await redis_client.srem(f"group:{group_id}", target_uid)
        await redis_client.delete(f"group_key:{group_id}:{target_uid}")
        await redis_client.srem(f"group_admins:{group_id}", target_uid)

        members = await redis_client.smembers(f"group:{group_id}")
        kick_msg = {
            "type":       "group_member_kicked",
            "group_id":   group_id,
            "uid":        target_uid,
            "by_uid":     my_uid,
            "time":       _now_ms(),
        }
        # Уведомляем кикнутого
        ws_target = self._cm.get_ws(target_uid)
        if ws_target:
            await self._cm.send_to(ws_target, kick_msg)
        # Уведомляем оставшихся
        for member in members:
            ws = self._cm.get_ws(member)
            if ws:
                await self._cm.send_to(ws, kick_msg)
        logger.info(f"🦵 {my_uid} kicked {target_uid} from group {group_id}")

    async def handle_promote(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        group_id   = data.get("group_id")
        target_uid = data.get("target_uid")
        if not redis_client or not group_id or not target_uid:
            return

        admins = await redis_client.smembers(f"group_admins:{group_id}")
        if my_uid not in admins:
            await self._cm.send_to(websocket, {"type": "error", "message": "Not an admin"})
            return

        await redis_client.sadd(f"group_admins:{group_id}", target_uid)
        notify = {
            "type":       "group_admin_promoted",
            "group_id":   group_id,
            "uid":        target_uid,
            "by_uid":     my_uid,
            "time":       _now_ms(),
        }
        members = await redis_client.smembers(f"group:{group_id}")
        for member in members:
            ws = self._cm.get_ws(member)
            if ws:
                await self._cm.send_to(ws, notify)
        logger.info(f"⭐ {my_uid} promoted {target_uid} in group {group_id}")

    async def handle_demote(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        """Снять администратора — только другой admin может."""
        group_id   = data.get("group_id")
        target_uid = data.get("target_uid")
        if not redis_client or not group_id or not target_uid:
            return

        admins = await redis_client.smembers(f"group_admins:{group_id}")
        if my_uid not in admins:
            await self._cm.send_to(websocket, {"type": "error", "message": "Not an admin"})
            return

        # Нельзя снять самого себя, если ты единственный админ
        if target_uid == my_uid and len(admins) <= 1:
            await self._cm.send_to(websocket, {"type": "error", "message": "Cannot demote the last admin"})
            return

        await redis_client.srem(f"group_admins:{group_id}", target_uid)
        notify = {
            "type":       "group_admin_demoted",
            "group_id":   group_id,
            "uid":        target_uid,
            "by_uid":     my_uid,
            "time":       _now_ms(),
        }
        members = await redis_client.smembers(f"group:{group_id}")
        for member in members:
            ws = self._cm.get_ws(member)
            if ws:
                await self._cm.send_to(ws, notify)
        logger.info(f"⬇️ {my_uid} demoted {target_uid} in group {group_id}")

    async def handle_delete_group(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        """Удалить группу — только создатель (первый admin) может."""
        group_id = data.get("group_id")
        if not redis_client or not group_id:
            return

        admins = await redis_client.smembers(f"group_admins:{group_id}")
        if my_uid not in admins:
            await self._cm.send_to(websocket, {"type": "error", "message": "Not an admin"})
            return

        members = await redis_client.smembers(f"group:{group_id}")
        delete_msg = {
            "type":     "group_deleted",
            "group_id": group_id,
            "by_uid":   my_uid,
            "time":     _now_ms(),
        }

        # Уведомляем всех участников
        for member in members:
            ws = self._cm.get_ws(member)
            if ws:
                await self._cm.send_to(ws, delete_msg)
            # Удаляем ключи участников
            await redis_client.delete(f"group_key:{group_id}:{member}")

        # Удаляем все данные группы из Redis
        await redis_client.delete(f"group:{group_id}")
        await redis_client.delete(f"group_name:{group_id}")
        await redis_client.delete(f"group_admins:{group_id}")
        await redis_client.delete(f"group_settings:{group_id}")

        logger.info(f"🗑️ {my_uid} deleted group {group_id}")

    async def handle_update_settings(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        group_id = data.get("group_id")
        if not redis_client or not group_id:
            return

        admins = await redis_client.smembers(f"group_admins:{group_id}")
        if my_uid not in admins:
            await self._cm.send_to(websocket, {"type": "error", "message": "Not an admin"})
            return

        settings = {}
        if "only_admins_post" in data:
            settings["only_admins_post"] = "1" if data["only_admins_post"] else "0"
        if settings:
            await redis_client.hset(f"group_settings:{group_id}", mapping=settings)
        await self._cm.send_to(websocket, {"type": "group_settings_updated", "group_id": group_id})
        logger.info(f"⚙️ {my_uid} updated settings for {group_id}: {settings}")

    async def handle_distribute_keys(self, my_uid: str, data: dict, redis_client):
        group_id       = data.get("group_id")
        encrypted_keys = data.get("encrypted_keys", {})
        if not redis_client or not group_id or not encrypted_keys:
            return

        for uid, blob in encrypted_keys.items():
            key = f"group_key:{group_id}:{uid}"
            await redis_client.setex(key, GROUP_KEY_TTL, json.dumps({
                "blob":    blob,
                "creator": my_uid,
            }))
        logger.info(f"🔑 Group keys stored for {group_id} ({len(encrypted_keys)} members)")

    async def handle_get_key(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        group_id = data.get("group_id")
        if not redis_client or not group_id:
            return

        raw = await redis_client.get(f"group_key:{group_id}:{my_uid}")
        if raw:
            entry = json.loads(raw)
            await self._cm.send_to(websocket, {
                "type":          "group_key_response",
                "group_id":      group_id,
                "encrypted_key": entry.get("blob"),
                "creator_uid":   entry.get("creator"),
            })
        else:
            await self._cm.send_to(websocket, {
                "type":     "group_key_not_found",
                "group_id": group_id,
            })
