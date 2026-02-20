import asyncio
import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, Optional

import redis.asyncio as redis
import firebase_admin
from firebase_admin import credentials, messaging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# ─── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DDChatRelay")

# ─── Приложение ─────────────────────────────────────────────────────────────
app = FastAPI(title="DeepDrift Secure Relay", version="4.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Конфигурация ───────────────────────────────────────────────────────────
REDIS_URL = os.environ.get("REDIS_URL")
FB_JSON   = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

UID_PATTERN = re.compile(r"^\d{6}$")  # UID — строго 6 цифр

# ─── Глобальное состояние ───────────────────────────────────────────────────
active_connections: Dict[str, WebSocket] = {}
redis_client: Optional[redis.Redis] = None

# Rate limiting: uid -> [timestamp, ...]
_rate_limit: Dict[str, list] = {}
RATE_LIMIT_MAX   = 60   # сообщений
RATE_LIMIT_WINDOW = 60  # секунд

# ─── Firebase ───────────────────────────────────────────────────────────────
try:
    if FB_JSON:
        fb_dict = json.loads(FB_JSON)
        cred = credentials.Certificate(fb_dict)
        firebase_admin.initialize_app(cred)
        logger.info("✅ Firebase Admin SDK initialized")
    else:
        logger.error("❌ FIREBASE_SERVICE_ACCOUNT_JSON is missing!")
except Exception as e:
    logger.error(f"❌ Firebase Error: {e}")


# ─── Redis ──────────────────────────────────────────────────────────────────
async def init_redis():
    global redis_client
    if not REDIS_URL:
        logger.warning("⚠️ REDIS_URL not set. Offline mode disabled.")
        return
    try:
        url = REDIS_URL.replace("cache://", "redis://")
        redis_client = redis.from_url(
            url,
            encoding="utf-8",
            decode_responses=True,
            socket_timeout=5.0,
            retry_on_timeout=True,
        )
        await redis_client.ping()
        logger.info("✅ Redis connected successfully!")
    except Exception as e:
        logger.error(f"❌ Redis Connection Failed: {e}")
        redis_client = None


@app.on_event("startup")
async def startup_event():
    await init_redis()


# ─── Хелперы ────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


def _is_valid_uid(uid: str) -> bool:
    return bool(uid and UID_PATTERN.match(str(uid)))


def _check_rate_limit(uid: str) -> bool:
    """Возвращает True если запрос разрешён."""
    now = datetime.now().timestamp()
    timestamps = _rate_limit.get(uid, [])
    # Удаляем устаревшие записи
    timestamps = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
    if len(timestamps) >= RATE_LIMIT_MAX:
        return False
    timestamps.append(now)
    _rate_limit[uid] = timestamps
    return True


async def _send_to(ws: WebSocket, payload: dict):
    """Безопасная отправка JSON клиенту."""
    try:
        await ws.send_text(json.dumps(payload))
    except Exception as e:
        logger.error(f"❌ send_to error: {e}")


async def _send_fcm_push(target_uid: str, from_uid: str, message_type: str = "new_message"):
    """Асинхронная FCM-пуш-нотификация (не блокирует event loop)."""
    if not redis_client or not firebase_admin._apps:
        return
    try:
        token = await redis_client.get(f"fcm_token:{target_uid}")
        if not token:
            return

        title_map = {
            "new_message":       f"DeepDrift: {from_uid[:8]}",
            "message_deleted":   "Message deleted",
            "message_edited":    "Message edited",
            "message_reaction":  "New reaction",
        }
        body_map = {
            "new_message":       "New encrypted message",
            "message_deleted":   "A message was deleted",
            "message_edited":    "A message was edited",
            "message_reaction":  "New reaction on your message",
        }

        msg = messaging.Message(
            notification=messaging.Notification(
                title=title_map.get(message_type, "DeepDrift"),
                body=body_map.get(message_type, "New event"),
            ),
            data={"from_uid": from_uid, "type": message_type},
            token=token,
        )
        # Запускаем синхронный вызов в thread pool, не блокируя event loop
        await asyncio.get_event_loop().run_in_executor(None, messaging.send, msg)
        logger.info(f"📲 Push sent to {target_uid} ({message_type})")
    except Exception as e:
        logger.error(f"❌ Push Send Error: {e}")


async def _send_offline_messages(websocket: WebSocket, my_uid: str):
    """Доставка оффлайн-сообщений при подключении."""
    if not redis_client:
        return
    await asyncio.sleep(0.5)
    try:
        offline_key = f"offline_queue:{my_uid}"
        messages = await redis_client.lrange(offline_key, 0, -1)
        if messages:
            logger.info(f"📬 Sending {len(messages)} offline messages to {my_uid}")
            for msg_json in messages:
                try:
                    await websocket.send_text(msg_json)
                    logger.info(f"📨 Sent offline message to {my_uid}")
                except Exception as e:
                    logger.error(f"❌ Failed to send offline message: {e}")
            await redis_client.delete(offline_key)
            logger.info(f"🗑️ Cleared offline queue for {my_uid}")
    except Exception as e:
        logger.error(f"❌ Error sending offline messages: {e}")


async def _store_offline_message(target_uid: str, message_data: dict):
    """Сохранение сообщения для оффлайн-доставки."""
    if not redis_client:
        return
    try:
        offline_key = f"offline_queue:{target_uid}"
        await redis_client.rpush(offline_key, json.dumps(message_data))
        await redis_client.expire(offline_key, 7 * 24 * 3600)
        logger.info(f"💾 Stored offline message for {target_uid}")
    except Exception as e:
        logger.error(f"❌ Failed to store offline message: {e}")


async def _deliver_or_store(target_uid: str, payload: dict, push_type: str, from_uid: str):
    """Доставить сообщение онлайн, либо сохранить оффлайн и отправить push."""
    if target_uid in active_connections:
        try:
            await active_connections[target_uid].send_text(json.dumps(payload))
            return True
        except Exception as e:
            logger.error(f"❌ Failed to deliver to {target_uid}: {e}")
    await _store_offline_message(target_uid, payload)
    await _send_fcm_push(target_uid, from_uid, push_type)
    return False


# ─── REST эндпоинты ─────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "status": "ONLINE",
        "version": "4.1.0",
        "firebase": "active" if firebase_admin._apps else "error",
        "redis": "connected" if redis_client else "disconnected",
        "users_online": len(active_connections),
        "features": [
            "delete_message", "edit_message", "message_reaction",
            "forward_message", "read_receipt", "delivery_receipt",
            "voice_messages", "photo_messages", "file_transfer",
            "server_ack", "rate_limiting", "uid_validation",
        ],
    }


# ─── WebSocket ──────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid: Optional[str] = None

    try:
        while True:
            raw  = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            # ── INIT ──────────────────────────────────────────────────────
            if msg_type == "init":
                uid_candidate = str(data.get("my_uid", "")).strip()

                if not _is_valid_uid(uid_candidate):
                    await _send_to(websocket, {
                        "type": "error",
                        "message": "my_uid must be a 6-digit number"
                    })
                    continue

                my_uid = uid_candidate
                active_connections[my_uid] = websocket
                logger.info(f"✅ {my_uid} connected (total: {len(active_connections)})")

                await _send_to(websocket, {
                    "type": "uid_assigned",
                    "my_uid": my_uid,
                })

                asyncio.create_task(_send_offline_messages(websocket, my_uid))
                continue

            # Все дальнейшие сообщения требуют авторизации
            if not my_uid:
                await _send_to(websocket, {
                    "type": "error",
                    "message": "Not initialized. Send init first."
                })
                continue

            # ── REGISTER FCM TOKEN ────────────────────────────────────────
            if msg_type == "register_fcm_token":
                token = data.get("fcm_token")
                if redis_client and token:
                    await redis_client.set(f"fcm_token:{my_uid}", token)
                    logger.info(f"📱 Token registered for {my_uid}")
                    await _send_to(websocket, {"type": "fcm_token_registered", "status": "success"})
                continue

            # ── REGISTER PUBLIC KEY ───────────────────────────────────────
            if msg_type == "register_public_key":
                x25519_key = data.get("x25519_key")
                ed25519_key = data.get("ed25519_key")

                if redis_client and x25519_key and ed25519_key:
                    await redis_client.setex(f"pubkey:{my_uid}:x25519",  30 * 24 * 3600, x25519_key)
                    await redis_client.setex(f"pubkey:{my_uid}:ed25519", 30 * 24 * 3600, ed25519_key)
                    logger.info(f"🔑 Public keys registered for {my_uid}")
                    await _send_to(websocket, {"type": "public_key_registered", "status": "success"})
                continue

            # ── REQUEST PUBLIC KEY ────────────────────────────────────────
            if msg_type == "request_public_key":
                target_uid = data.get("target_uid")

                if redis_client and target_uid:
                    try:
                        x25519_key  = await redis_client.get(f"pubkey:{target_uid}:x25519")
                        ed25519_key = await redis_client.get(f"pubkey:{target_uid}:ed25519")

                        if x25519_key and ed25519_key:
                            await _send_to(websocket, {
                                "type": "public_key_response",
                                "target_uid": target_uid,
                                "x25519_key": x25519_key,
                                "ed25519_key": ed25519_key,
                            })
                            logger.info(f"🔑 Sent public keys of {target_uid} to {my_uid}")
                        else:
                            await _send_to(websocket, {
                                "type": "public_key_response",
                                "target_uid": target_uid,
                                "error": "keys_not_found",
                            })
                    except Exception as e:
                        logger.error(f"❌ Error retrieving keys: {e}")
                continue

            # ── MESSAGE ───────────────────────────────────────────────────
            if msg_type == "message":
                # Rate limiting
                if not _check_rate_limit(my_uid):
                    await _send_to(websocket, {"type": "error", "message": "Rate limit exceeded"})
                    continue

                target_uid     = data.get("target_uid")
                encrypted_text = data.get("encrypted_text")
                signature      = data.get("signature")
                message_id     = data.get("id")
                reply_to_id    = data.get("replyToId")
                message_type   = data.get("messageType", "text")
                media_data     = data.get("mediaData")
                file_name      = data.get("fileName")
                file_size      = data.get("fileSize")
                mime_type      = data.get("mimeType")

                if not all([target_uid, encrypted_text, message_id]):
                    continue

                payload = {
                    "type":          "message",
                    "from_uid":      my_uid,
                    "id":            message_id,
                    "encrypted_text": encrypted_text,
                    "signature":     signature,
                    "time":          _now_ms(),
                    "replyToId":     reply_to_id,
                    "messageType":   message_type,
                    "mediaData":     media_data,
                    "fileName":      file_name,
                    "fileSize":      file_size,
                    "mimeType":      mime_type,
                }

                delivered = await _deliver_or_store(target_uid, payload, "new_message", my_uid)

                # ✅ server_ack — клиент ждёт это подтверждение
                await _send_to(websocket, {
                    "type":             "server_ack",
                    "id":               message_id,
                    "delivered_online": delivered,
                })
                logger.info(f"📨 Message {message_id}: {'online' if delivered else 'offline'}")
                continue

            # ── DELETE MESSAGE ────────────────────────────────────────────
            if msg_type == "delete_message":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")

                if not all([target_uid, message_id]):
                    continue

                payload = {
                    "type":       "message_deleted",
                    "from_uid":   my_uid,
                    "message_id": message_id,
                    "time":       _now_ms(),
                }
                await _deliver_or_store(target_uid, payload, "message_deleted", my_uid)
                logger.info(f"🗑️ Delete request: {message_id}")
                continue

            # ── EDIT MESSAGE ──────────────────────────────────────────────
            if msg_type == "edit_message":
                target_uid        = data.get("target_uid")
                message_id        = data.get("message_id")
                new_encrypted_text = data.get("new_encrypted_text")
                new_signature      = data.get("new_signature")

                if not all([target_uid, message_id, new_encrypted_text]):
                    continue

                payload = {
                    "type":              "message_edited",
                    "from_uid":          my_uid,
                    "message_id":        message_id,
                    "new_encrypted_text": new_encrypted_text,
                    "new_signature":      new_signature,
                    "time":              _now_ms(),
                }
                await _deliver_or_store(target_uid, payload, "message_edited", my_uid)
                logger.info(f"✏️ Edit delivered: {message_id}")
                continue

            # ── MESSAGE REACTION ──────────────────────────────────────────
            if msg_type == "message_reaction":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                emoji      = data.get("emoji")
                action     = data.get("action")  # 'add' | 'remove'

                if not all([target_uid, message_id, emoji, action]):
                    continue

                payload = {
                    "type":       "message_reaction",
                    "from_uid":   my_uid,
                    "message_id": message_id,
                    "emoji":      emoji,
                    "action":     action,
                    "time":       _now_ms(),
                }
                if target_uid in active_connections:
                    await _send_to(active_connections[target_uid], payload)
                    logger.info(f"{emoji} Reaction delivered: {emoji} on {message_id}")
                else:
                    await _send_fcm_push(target_uid, my_uid, "message_reaction")
                continue

            # ── FORWARD MESSAGE ───────────────────────────────────────────
            if msg_type == "forward_message":
                target_uid          = data.get("target_uid")
                original_message_id = data.get("original_message_id")
                forwarded_from      = data.get("forwarded_from")
                encrypted_text      = data.get("encrypted_text")
                signature           = data.get("signature")
                new_message_id      = data.get("id")

                if not all([target_uid, original_message_id, encrypted_text, new_message_id]):
                    continue

                payload = {
                    "type":                "message",
                    "from_uid":            my_uid,
                    "id":                  new_message_id,
                    "encrypted_text":      encrypted_text,
                    "signature":           signature,
                    "time":                _now_ms(),
                    "forwarded_from":      forwarded_from,
                    "original_message_id": original_message_id,
                }
                await _deliver_or_store(target_uid, payload, "new_message", my_uid)

                await _send_to(websocket, {
                    "type":             "server_ack",
                    "id":               new_message_id,
                    "delivered_online": target_uid in active_connections,
                })
                logger.info(f"↪️ Forward delivered: {new_message_id}")
                continue

            # ── READ RECEIPT ──────────────────────────────────────────────
            if msg_type == "read_receipt":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")

                if not all([target_uid, message_id]):
                    continue

                payload = {
                    "type":       "read_receipt",
                    "from_uid":   my_uid,
                    "message_id": message_id,
                    "time":       _now_ms(),
                }
                if target_uid in active_connections:
                    await _send_to(active_connections[target_uid], payload)
                    logger.info(f"✓✓ Read receipt delivered: {message_id}")
                continue

            # ── DELIVERY RECEIPT ──────────────────────────────────────────
            if msg_type == "delivery_receipt":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")

                if not all([target_uid, message_id]):
                    continue

                payload = {
                    "type":       "delivery_receipt",
                    "from_uid":   my_uid,
                    "message_id": message_id,
                    "time":       _now_ms(),
                }
                if target_uid in active_connections:
                    await _send_to(active_connections[target_uid], payload)
                    logger.info(f"✓ Delivery receipt sent: {message_id}")
                continue

            # ── TYPING INDICATOR ──────────────────────────────────────────
            if msg_type == "typing_indicator":
                target_uid = data.get("target_uid")
                typing     = data.get("typing", False)

                if target_uid and target_uid in active_connections:
                    await _send_to(active_connections[target_uid], {
                        "type":     "typing_indicator",
                        "from_uid": my_uid,
                        "typing":   typing,
                    })
                continue

            # ── PING ──────────────────────────────────────────────────────
            if msg_type == "ping":
                await _send_to(websocket, {"type": "pong"})
                continue

            logger.warning(f"⚠️ Unknown message type: {msg_type} from {my_uid}")

    except WebSocketDisconnect:
        if my_uid:
            active_connections.pop(my_uid, None)
            logger.info(f"👋 {my_uid} disconnected (total: {len(active_connections)})")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
        if my_uid:
            active_connections.pop(my_uid, None)
