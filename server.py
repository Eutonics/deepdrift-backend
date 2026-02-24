import asyncio
import json
import logging
import os
import re
import shutil
import uuid
from datetime import datetime
from typing import Dict, Optional

import redis.asyncio as redis
import firebase_admin
from firebase_admin import credentials, messaging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

# ─── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DDChatRelay")

# ─── Приложение ─────────────────────────────────────────────────────────────
app = FastAPI(title="DeepDrift Secure Relay", version="4.4.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Конфигурация ───────────────────────────────────────────────────────────
REDIS_URL = os.environ.get("REDIS_URL")
FB_JSON   = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

UID_PATTERN = re.compile(r"^\d{6}$")  # UID — строго 6 цифр

# ─── Директория для файлов ──────────────────────────────────────────────────
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

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
        logger.warning("⚠️ FIREBASE_SERVICE_ACCOUNT_JSON is missing! Push notifications disabled.")
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
    timestamps = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
    if len(timestamps) >= RATE_LIMIT_MAX:
        return False
    timestamps.append(now)
    _rate_limit[uid] = timestamps
    return True

def _clean_rate_limit(uid: str):
    """Очистка памяти при отключении."""
    if uid in _rate_limit:
        del _rate_limit[uid]

async def _send_to(ws: WebSocket, payload: dict):
    """Безопасная отправка JSON клиенту (для текущего соединения)."""
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
            "new_message":       "New encrypted message or media",
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
            android=messaging.AndroidConfig(priority='high'),
            apns=messaging.APNSConfig(
                payload=messaging.APNSPayload(
                    aps=messaging.Aps(content_available=True)
                )
            )
        )
        
        await asyncio.get_event_loop().run_in_executor(None, messaging.send, msg)
        logger.info(f"📲 Push sent to {target_uid} ({message_type})")
    except Exception as e:
        logger.error(f"❌ Push Send Error: {e}")


# ─── Оффлайн сообщения ──────────────────────────────────────────────────────

async def _send_offline_messages(websocket: WebSocket, my_uid: str):
    """Доставка ВСЕХ оффлайн-сообщений при подключении (Legacy/Init)."""
    if not redis_client:
        return
    await asyncio.sleep(0.5)
    try:
        offline_key = f"offline_queue:{my_uid}"
        messages = await redis_client.lrange(offline_key, 0, -1)
        if messages:
            logger.info(f"📬 Sending {len(messages)} global offline messages to {my_uid}")
            for msg_json in messages:
                try:
                    await websocket.send_text(msg_json)
                except Exception as e:
                    logger.error(f"❌ Failed to send offline message: {e}")
            await redis_client.delete(offline_key)
            logger.info(f"🗑️ Cleared global offline queue for {my_uid}")
    except Exception as e:
        logger.error(f"❌ Error sending offline messages: {e}")

async def _send_offline_messages_from(websocket: WebSocket, my_uid: str, from_uid: str):
    """Доставка оффлайн-сообщений от конкретного отправителя."""
    if not redis_client:
        return
    try:
        offline_key = f"offline:{my_uid}:from:{from_uid}"
        messages = await redis_client.lrange(offline_key, 0, -1)
        
        if messages:
            logger.info(f"📬 Sending {len(messages)} specific offline messages from {from_uid} to {my_uid}")
            for msg_json in messages:
                try:
                    await websocket.send_text(msg_json)
                except Exception as e:
                    logger.error(f"❌ Failed to send specific offline message: {e}")
            
            await redis_client.delete(offline_key)
            logger.info(f"🗑️ Cleared specific offline queue for {my_uid} from {from_uid}")
        else:
            logger.debug(f"📭 No specific offline messages from {from_uid} for {my_uid}")
            
    except Exception as e:
        logger.error(f"❌ Error sending specific offline messages from {from_uid}: {e}")

async def _store_offline_message(target_uid: str, message_data: dict):
    """Сохранение сообщения для оффлайн-доставки (Dual Storage)."""
    if not redis_client:
        return
    try:
        from_uid = message_data.get("from_uid", "unknown")
        
        # 1. Сохраняем в общую очередь
        offline_key_global = f"offline_queue:{target_uid}"
        await redis_client.rpush(offline_key_global, json.dumps(message_data))
        await redis_client.expire(offline_key_global, 7 * 24 * 3600)
        
        # 2. Сохраняем по отправителям
        offline_key_specific = f"offline:{target_uid}:from:{from_uid}"
        await redis_client.rpush(offline_key_specific, json.dumps(message_data))
        await redis_client.expire(offline_key_specific, 7 * 24 * 3600)
        
        logger.info(f"💾 Stored offline message for {target_uid} from {from_uid}")
    except Exception as e:
        logger.error(f"❌ Failed to store offline message: {e}")

async def _deliver_or_store(target_uid: str, payload: dict, push_type: str, from_uid: str):
    """
    Пытается доставить сообщение онлайн. 
    Если не выходит (ошибка сокета) — СРАЗУ удаляет сокет и сохраняет в оффлайн + пуш.
    """
    
    # 1. Попытка онлайн доставки
    if target_uid in active_connections:
        ws = active_connections[target_uid]
        try:
            # Проверка состояния сокета перед отправкой (косвенная)
            if ws.client_state.name != "CONNECTED":
                raise WebSocketDisconnect("Socket not connected")
                
            await ws.send_text(json.dumps(payload))
            return True  # Успешно доставлено онлайн
        except Exception as e:
            logger.error(f"❌ Failed to deliver to {target_uid}: {e}")
            
            # 🔥 CRITICAL FIX: Если отправка не удалась, значит сокет мертв.
            # Удаляем его НЕМЕДЛЕННО, чтобы не пытаться слать туда снова
            # и чтобы следующие сообщения сразу шли в оффлайн/пуш.
            logger.warning(f"🔌 Removing dead connection for {target_uid}")
            if target_uid in active_connections:
                del active_connections[target_uid]
            
            # Пропускаем flow дальше к сохранению в оффлайн
    
    # 2. Если пользователя нет в active_connections ИЛИ отправка упала с ошибкой:
    logger.info(f"💤 User {target_uid} is offline/unreachable. Storing & Pushing.")
    
    await _store_offline_message(target_uid, payload)
    await _send_fcm_push(target_uid, from_uid, push_type)
    
    return False


# ─── REST эндпоинты (HTTP) ──────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "status": "ONLINE",
        "version": "4.4.0",
        "firebase": "active" if firebase_admin._apps else "error/disabled",
        "redis": "connected" if redis_client else "disconnected",
        "users_online": len(active_connections),
        "features": [
            "http_file_transfer", "dual_offline_storage", "request_offline_messages",
            "delete_message", "edit_message", "message_reaction",
            "forward_message", "read_receipt", "delivery_receipt",
            "server_ack", "rate_limiting",
        ],
    }

# HTTP загрузка файла
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Генерируем уникальное имя файла
        file_id = f"{uuid.uuid4().hex}_{file.filename}"
        file_path = os.path.join(UPLOAD_DIR, file_id)
        
        # Сохраняем на диск сервера
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        logger.info(f"📁 HTTP File uploaded: {file_id}")
        return {"status": "success", "file_id": file_id}
    except Exception as e:
        logger.error(f"❌ HTTP Upload error: {e}")
        return {"status": "error", "message": str(e)}

# HTTP скачивание файла
@app.get("/download/{file_id}")
async def download_file(file_id: str):
    # Защита от выхода за пределы директории
    safe_file_id = os.path.basename(file_id) 
    file_path = os.path.join(UPLOAD_DIR, safe_file_id)
    
    if os.path.exists(file_path):
        return FileResponse(file_path)
        
    logger.warning(f"⚠️ HTTP Download failed (not found): {safe_file_id}")
    return {"status": "error", "message": "File not found"}


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

            if not my_uid:
                await _send_to(websocket, {"type": "error", "message": "Not initialized. Send init first."})
                continue

            # ── REQUEST OFFLINE MESSAGES ──────────────────────────────────
            if msg_type == "request_offline_messages":
                target_from_uid = data.get("target_uid") or data.get("from_uid")
                if not target_from_uid:
                    continue
                logger.info(f"📥 {my_uid} requested offline messages from {target_from_uid}")
                await _send_offline_messages_from(websocket, my_uid, target_from_uid)
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
                if not _check_rate_limit(my_uid):
                    await _send_to(websocket, {"type": "error", "message": "Rate limit exceeded"})
                    continue

                target_uid     = data.get("target_uid")
                encrypted_text = data.get("encrypted_text")
                signature      = data.get("signature")
                message_id     = data.get("id")
                reply_to_id    = data.get("replyToId")
                message_type   = data.get("messageType", "text")
                media_data     = data.get("mediaData") # Теперь тут прилетает 'FILE_ID:...'
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
                if not all([target_uid, message_id]): continue
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

                if not all([target_uid, message_id, new_encrypted_text]): continue

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
                action     = data.get("action")

                if not all([target_uid, message_id, emoji, action]): continue

                payload = {
                    "type":       "message_reaction",
                    "from_uid":   my_uid,
                    "message_id": message_id,
                    "emoji":      emoji,
                    "action":     action,
                    "time":       _now_ms(),
                }
                await _deliver_or_store(target_uid, payload, "message_reaction", my_uid)
                logger.info(f"{emoji} Reaction handled: {emoji} on {message_id}")
                continue

            # ── FORWARD MESSAGE ───────────────────────────────────────────
            if msg_type == "forward_message":
                target_uid          = data.get("target_uid")
                original_message_id = data.get("original_message_id")
                forwarded_from      = data.get("forwarded_from")
                encrypted_text      = data.get("encrypted_text")
                signature           = data.get("signature")
                new_message_id      = data.get("id")

                if not all([target_uid, original_message_id, encrypted_text, new_message_id]): continue

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
                delivered = await _deliver_or_store(target_uid, payload, "new_message", my_uid)

                await _send_to(websocket, {
                    "type":             "server_ack",
                    "id":               new_message_id,
                    "delivered_online": delivered,
                })
                logger.info(f"↪️ Forward delivered: {new_message_id}")
                continue

            # ── READ RECEIPT ──────────────────────────────────────────────
            if msg_type == "read_receipt":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                if not all([target_uid, message_id]): continue

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
                if not all([target_uid, message_id]): continue

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
            if my_uid in active_connections and active_connections[my_uid] == websocket:
                active_connections.pop(my_uid, None)
                logger.info(f"👋 {my_uid} disconnected (total: {len(active_connections)})")
            _clean_rate_limit(my_uid)
    except Exception as e:
        logger.error(f"❌ WebSocket loop error: {e}")
        if my_uid and my_uid in active_connections and active_connections[my_uid] == websocket:
            active_connections.pop(my_uid, None)
            _clean_rate_limit(my_uid)
