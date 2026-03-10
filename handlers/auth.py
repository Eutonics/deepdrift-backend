"""
Аутентификация: Challenge-Response через Ed25519.
"""
import asyncio
import base64
import logging
import secrets
from typing import Optional

from fastapi import WebSocket
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.exceptions import InvalidSignature

from config import NONCE_TTL_SECONDS, NONCE_SIZE_BYTES, UPLOAD_TOKEN_TTL, UID_PATTERN

logger = logging.getLogger("DDChatRelay")


class AuthHandler:
    def __init__(self, connection_manager, offline_queue):
        self._cm = connection_manager
        self._oq = offline_queue

    @staticmethod
    def is_valid_uid(uid: str) -> bool:
        return bool(uid and UID_PATTERN.match(str(uid)))

    async def assign_uid(self, websocket: WebSocket, uid: str, redis_client):
        """Финализирует подключение: регистрирует соединение, шлёт uid_assigned."""
        await self._cm.register(uid, websocket)
        await self._update_last_seen(uid, redis_client)
        logger.info(f"✅ {uid} authenticated & connected (total: {self._cm.online_count})")

        # Генерируем upload_token
        upload_token = secrets.token_urlsafe(32)
        if redis_client:
            try:
                await redis_client.setex(f"upload_token:{upload_token}", UPLOAD_TOKEN_TTL, uid)
            except Exception:
                pass

        await self._cm.send_to(websocket, {
            "type":         "uid_assigned",
            "my_uid":       uid,
            "upload_token": upload_token,
        })
        asyncio.create_task(self._oq.send_all(websocket, uid, redis_client))

    async def handle_init(self, websocket: WebSocket, uid_candidate: str, redis_client) -> Optional[str]:
        """
        Обрабатывает init.
        Возвращает uid если аутентификация прошла сразу, None если нужен challenge.
        """
        if not self.is_valid_uid(uid_candidate):
            await self._cm.send_to(websocket, {"type": "error", "message": "Invalid UID format (6 digits required)"})
            return None

        if not redis_client:
            await self.assign_uid(websocket, uid_candidate, redis_client)
            return uid_candidate

        stored_pubkey = await redis_client.get(f"auth:pubkey:{uid_candidate}")

        if stored_pubkey is None:
            await self.assign_uid(websocket, uid_candidate, redis_client)
            return uid_candidate
        else:
            nonce     = secrets.token_bytes(NONCE_SIZE_BYTES)
            nonce_b64 = base64.b64encode(nonce).decode()
            await redis_client.setex(f"auth:nonce:{uid_candidate}", NONCE_TTL_SECONDS, nonce_b64)
            await self._cm.send_to(websocket, {"type": "auth_challenge", "nonce": nonce_b64})
            logger.info(f"🔑 Auth challenge issued for {uid_candidate}")
            return None

    async def handle_auth_response(self, websocket: WebSocket, data: dict, redis_client) -> Optional[str]:
        """Проверяет подпись нонса. Возвращает uid при успехе."""
        uid       = str(data.get("uid", "")).strip()
        nonce_b64 = data.get("nonce")
        sig_b64   = data.get("signature")

        if not all([uid, nonce_b64, sig_b64]):
            await self._cm.send_to(websocket, {"type": "auth_failed", "reason": "missing_fields"})
            return None

        if not redis_client:
            await self.assign_uid(websocket, uid, redis_client)
            return uid

        stored_nonce = await redis_client.get(f"auth:nonce:{uid}")
        if stored_nonce is None:
            await self._cm.send_to(websocket, {"type": "auth_failed", "reason": "nonce_expired"})
            logger.warning(f"🚫 Auth failed for {uid}: nonce expired")
            return None
        if stored_nonce != nonce_b64:
            await self._cm.send_to(websocket, {"type": "auth_failed", "reason": "nonce_mismatch"})
            logger.warning(f"🚫 Auth failed for {uid}: nonce mismatch")
            return None

        await redis_client.delete(f"auth:nonce:{uid}")

        stored_pubkey_b64 = await redis_client.get(f"auth:pubkey:{uid}")
        if stored_pubkey_b64 is None:
            await self._cm.send_to(websocket, {"type": "auth_failed", "reason": "not_registered"})
            return None

        try:
            pubkey_bytes = base64.b64decode(stored_pubkey_b64)
            sig_bytes    = base64.b64decode(sig_b64)
            nonce_bytes  = base64.b64decode(nonce_b64)

            pubkey = Ed25519PublicKey.from_public_bytes(pubkey_bytes)
            pubkey.verify(sig_bytes, nonce_bytes)

            await self.assign_uid(websocket, uid, redis_client)
            return uid

        except InvalidSignature:
            await self._cm.send_to(websocket, {"type": "auth_failed", "reason": "invalid_signature"})
            logger.warning(f"🚫 Auth failed for {uid}: invalid Ed25519 signature")
            return None
        except Exception as e:
            await self._cm.send_to(websocket, {"type": "auth_failed", "reason": "verification_error"})
            logger.error(f"❌ Auth verification error for {uid}: {e}")
            return None

    async def handle_register(self, websocket: WebSocket, uid: str, data: dict, redis_client):
        """Привязывает Ed25519 pubkey к uid."""
        pubkey_b64 = data.get("ed25519_pubkey")

        if not pubkey_b64:
            await self._cm.send_to(websocket, {"type": "error", "reason": "missing ed25519_pubkey"})
            return

        try:
            pubkey_bytes = base64.b64decode(pubkey_b64)
            if len(pubkey_bytes) != 32:
                raise ValueError("Ed25519 pubkey must be 32 bytes")
            Ed25519PublicKey.from_public_bytes(pubkey_bytes)
        except Exception as e:
            await self._cm.send_to(websocket, {"type": "error", "reason": f"invalid_pubkey: {e}"})
            return

        if not redis_client:
            await self._cm.send_to(websocket, {"type": "registered", "uid": uid})
            return

        stored = await redis_client.get(f"auth:pubkey:{uid}")

        if stored is None:
            await redis_client.set(f"auth:pubkey:{uid}", pubkey_b64)
            await self._cm.send_to(websocket, {"type": "registered", "uid": uid})
            logger.info(f"📝 New account registered: {uid}")
        elif stored == pubkey_b64:
            await self._cm.send_to(websocket, {"type": "registered", "uid": uid})
            logger.info(f"📝 Account re-registered (same key): {uid}")
        else:
            await self._cm.send_to(websocket, {"type": "uid_taken", "reason": "uid already registered with a different key"})
            logger.warning(f"🚫 uid_taken: {uid} tried to register with different pubkey")

    @staticmethod
    async def _update_last_seen(uid: str, redis_client):
        if redis_client:
            try:
                from datetime import datetime
                now_ms = int(datetime.now().timestamp() * 1000)
                await redis_client.set(f"last_seen:{uid}", now_ms)
            except Exception:
                pass

    @staticmethod
    async def validate_upload_token(token: str | None, redis_client) -> bool:
        """
        Проверяет upload_token.
        SECURITY FIX: возвращает False если Redis недоступен (вместо True).
        """
        if not redis_client:
            return False  # Без Redis не можем проверить — блокируем
        if not token:
            return False
        try:
            uid = await redis_client.get(f"upload_token:{token}")
            return uid is not None
        except Exception:
            return False  # Redis недоступен — блокируем для безопасности
