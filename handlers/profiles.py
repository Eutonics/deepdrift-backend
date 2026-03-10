"""
Профили, статусы, публичные ключи, блокировка пользователей.
"""
import logging
from datetime import datetime

from fastapi import WebSocket

logger = logging.getLogger("DDChatRelay")


def _now_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


class ProfileHandler:
    def __init__(self, connection_manager):
        self._cm = connection_manager

    async def handle_update_profile(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        nickname  = data.get("nickname") or ""
        avatar_id = data.get("avatar_id") or ""
        if redis_client:
            await redis_client.hset(f"profile:{my_uid}", mapping={"nickname": nickname, "avatar_id": avatar_id})
            await self._cm.send_to(websocket, {"type": "profile_updated", "status": "success"})

    async def handle_get_profile(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        target_uid = data.get("target_uid")
        if not redis_client or not target_uid:
            return

        if str(target_uid).startswith("g_"):
            group_name = await redis_client.get(f"group_name:{target_uid}") or target_uid
            members    = list(await redis_client.smembers(f"group:{target_uid}"))
            admins     = list(await redis_client.smembers(f"group_admins:{target_uid}"))
            await self._cm.send_to(websocket, {
                "type":       "profile_response",
                "uid":        target_uid,
                "nickname":   group_name,
                "group_name": group_name,
                "members":    members,
                "admins":     admins,
                "is_admin":   my_uid in admins,
                "is_group":   True,
            })
        else:
            prof      = await redis_client.hgetall(f"profile:{target_uid}")
            is_online = self._cm.is_online(target_uid)
            last_seen = await redis_client.get(f"last_seen:{target_uid}")
            await self._cm.send_to(websocket, {
                "type":      "profile_response",
                "uid":       target_uid,
                "nickname":  prof.get("nickname", target_uid),
                "avatar_id": prof.get("avatar_id", ""),
                "status":    "online" if is_online else "offline",
                "last_seen": int(last_seen) if last_seen else 0,
            })

    async def handle_check_statuses(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        uids = data.get("uids", [])
        if not redis_client or not isinstance(uids, list):
            return
        for u in uids:
            if str(u).startswith("g_"):
                continue
            is_online = self._cm.is_online(u)
            last_seen = await redis_client.get(f"last_seen:{u}")
            await self._cm.send_to(websocket, {
                "type":      "user_status",
                "uid":       u,
                "status":    "online" if is_online else "offline",
                "last_seen": int(last_seen) if last_seen else 0,
            })

    async def handle_register_public_key(self, my_uid: str, data: dict, redis_client):
        x25519_key  = data.get("x25519_key")
        ed25519_key = data.get("ed25519_key")
        if redis_client and x25519_key and ed25519_key:
            await redis_client.setex(f"pubkey:{my_uid}:x25519",  30 * 24 * 3600, x25519_key)
            await redis_client.setex(f"pubkey:{my_uid}:ed25519", 30 * 24 * 3600, ed25519_key)

    async def handle_request_public_key(self, websocket: WebSocket, data: dict, redis_client):
        target_uid = data.get("target_uid")
        if not redis_client or not target_uid:
            return
        x25519_key  = await redis_client.get(f"pubkey:{target_uid}:x25519")
        ed25519_key = await redis_client.get(f"pubkey:{target_uid}:ed25519")
        if x25519_key and ed25519_key:
            await self._cm.send_to(websocket, {
                "type":        "public_key_response",
                "target_uid":  target_uid,
                "x25519_key":  x25519_key,
                "ed25519_key": ed25519_key,
            })

    async def handle_register_fcm(self, my_uid: str, data: dict, redis_client):
        token = data.get("fcm_token")
        if redis_client and token:
            await redis_client.set(f"fcm_token:{my_uid}", token)

    # ── Блокировка пользователей ────────────────────────────────────────────
    # Заблокированный пользователь не может отправить сообщения блокирующему.
    # Redis set: blocked:{my_uid} → {uid1, uid2, ...}

    async def handle_block_user(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        """Заблокировать пользователя."""
        target_uid = data.get("target_uid")
        if not redis_client or not target_uid:
            return
        await redis_client.sadd(f"blocked:{my_uid}", target_uid)
        await self._cm.send_to(websocket, {
            "type":       "user_blocked",
            "target_uid": target_uid,
            "time":       _now_ms(),
        })
        logger.info(f"🚫 {my_uid} blocked {target_uid}")

    async def handle_unblock_user(self, websocket: WebSocket, my_uid: str, data: dict, redis_client):
        """Разблокировать пользователя."""
        target_uid = data.get("target_uid")
        if not redis_client or not target_uid:
            return
        await redis_client.srem(f"blocked:{my_uid}", target_uid)
        await self._cm.send_to(websocket, {
            "type":       "user_unblocked",
            "target_uid": target_uid,
            "time":       _now_ms(),
        })
        logger.info(f"✅ {my_uid} unblocked {target_uid}")

    async def handle_get_blocked(self, websocket: WebSocket, my_uid: str, redis_client):
        """Получить список заблокированных."""
        if not redis_client:
            return
        blocked = list(await redis_client.smembers(f"blocked:{my_uid}"))
        await self._cm.send_to(websocket, {
            "type":    "blocked_list",
            "blocked": blocked,
        })

    @staticmethod
    async def is_blocked(redis_client, blocker_uid: str, target_uid: str) -> bool:
        """Проверяет, заблокировал ли blocker_uid target_uid."""
        if not redis_client:
            return False
        try:
            return await redis_client.sismember(f"blocked:{blocker_uid}", target_uid)
        except Exception:
            return False
