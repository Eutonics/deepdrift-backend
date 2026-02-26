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
app = FastAPI(title="DeepDrift Secure Relay", version="4.5.0")
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

_rate_limit: Dict[str, list] = {}
RATE_LIMIT_MAX   = 60   
RATE_LIMIT_WINDOW = 60  

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
    now = datetime.now().timestamp()
    timestamps = _rate_limit.get(uid, [])
    timestamps = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
    if len(timestamps) >= RATE_LIMIT_MAX:
        return False
    timestamps.append(now)
    _rate_limit[uid] = timestamps
    return True

def _clean_rate_limit(uid: str):
    if uid in _rate_limit:
        del _rate_limit[uid]

async def _send_to(ws: WebSocket, payload: dict):
    try:
        await ws.send_text(json.dumps(payload))
    except Exception as e:
        logger.error(f"❌ send_to error: {e}")

async def _update_last_seen(uid: str):
    """Обновляет время последней активности пользователя в Redis"""
    if redis_client:
        try:
            await redis_client.set(f"last_seen:{uid}", _now_ms())
        except Exception:
            pass

# ─── Push и Оффлайн сообщения ───────────────────────────────────────────────

async def _send_fcm_push(target_uid: str, from_uid: str, message_type: str = "new_message"):
    if not redis_client or not firebase_admin._apps:
        return
    try:
        token = await redis_client.get(f"fcm_token:{target_uid}")
        if not token:
            return

        # Попытка получить никнейм отправителя для красивого пуша
        sender_profile = await redis_client.hgetall(f"profile:{from_uid}")
        sender_name = sender_profile.get("nickname", from_uid) if sender_profile else from_uid

        title_map = {
            "new_message":       f"DDChat: {sender_name}",
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
                title=title_map.get(message_type, "DDChat"),
                body=body_map.get(message_type, "New event"),
            ),
            data={"from_uid": from_uid, "type": message_type},
            token=token,
            android=messaging.AndroidConfig(priority='high'),
            apns=messaging.APNSConfig(payload=messaging.APNSPayload(aps=messaging.Aps(content_available=True)))
        )
        
        await asyncio.get_event_loop().run_in_executor(None, messaging.send, msg)
        logger.info(f"📲 Push sent to {target_uid} ({message_type})")
    except Exception as e:
        logger.error(f"❌ Push Send Error: {e}")


async def _send_offline_messages(websocket: WebSocket, my_uid: str):
    if not redis_client: return
    await asyncio.sleep(0.5)
    try:
        offline_key = f"offline_queue:{my_uid}"
        messages = await redis_client.lrange(offline_key, 0, -1)
        if messages:
            for msg_json in messages:
                try: await websocket.send_text(msg_json)
                except Exception: pass
            await redis_client.delete(offline_key)
    except Exception as e:
        logger.error(f"❌ Error sending offline messages: {e}")

async def _send_offline_messages_from(websocket: WebSocket, my_uid: str, from_uid: str):
    if not redis_client: return
    try:
        offline_key = f"offline:{my_uid}:from:{from_uid}"
        messages = await redis_client.lrange(offline_key, 0, -1)
        if messages:
            for msg_json in messages:
                try: await websocket.send_text(msg_json)
                except Exception: pass
            await redis_client.delete(offline_key)
    except Exception as e:
        logger.error(f"❌ Error sending specific offline messages: {e}")

async def _store_offline_message(target_uid: str, message_data: dict):
    if not redis_client: return
    try:
        from_uid = message_data.get("from_uid", "unknown")
        
        offline_key_global = f"offline_queue:{target_uid}"
        await redis_client.rpush(offline_key_global, json.dumps(message_data))
        await redis_client.expire(offline_key_global, 7 * 24 * 3600)
        
        offline_key_specific = f"offline:{target_uid}:from:{from_uid}"
        await redis_client.rpush(offline_key_specific, json.dumps(message_data))
        await redis_client.expire(offline_key_specific, 7 * 24 * 3600)
    except Exception as e:
        logger.error(f"❌ Failed to store offline message: {e}")

async def _deliver_or_store(target_uid: str, payload: dict, push_type: str, from_uid: str):
    if target_uid in active_connections:
        ws = active_connections[target_uid]
        try:
            if ws.client_state.name != "CONNECTED":
                raise WebSocketDisconnect("Socket not connected")
            await ws.send_text(json.dumps(payload))
            return True 
        except Exception as e:
            if target_uid in active_connections:
                del active_connections[target_uid]
    
    await _store_offline_message(target_uid, payload)
    await _send_fcm_push(target_uid, from_uid, push_type)
    return False


# ─── REST эндпоинты (HTTP) ──────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "status": "ONLINE",
        "version": "4.5.0",
        "firebase": "active" if firebase_admin._apps else "error/disabled",
        "redis": "connected" if redis_client else "disconnected",
        "users_online": len(active_connections),
    }

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_id = f"{uuid.uuid4().hex}_{file.filename}"
        file_path = os.path.join(UPLOAD_DIR, file_id)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"status": "success", "file_id": file_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    safe_file_id = os.path.basename(file_id) 
    file_path = os.path.join(UPLOAD_DIR, safe_file_id)
    if os.path.exists(file_path):
        return FileResponse(file_path)
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
                    await _send_to(websocket, {"type": "error", "message": "Invalid UID"})
                    continue

                my_uid = uid_candidate
                active_connections[my_uid] = websocket
                await _update_last_seen(my_uid) # Обновляем онлайн статус
                
                logger.info(f"✅ {my_uid} connected (total: {len(active_connections)})")

                await _send_to(websocket, {"type": "uid_assigned", "my_uid": my_uid})
                asyncio.create_task(_send_offline_messages(websocket, my_uid))
                continue

            if not my_uid:
                await _send_to(websocket, {"type": "error", "message": "Not initialized."})
                continue

            await _update_last_seen(my_uid) # Любое действие обновляет статус

            # ── ПРОФИЛИ И СТАТУСЫ (НОВЫЙ ФУНКЦИОНАЛ) ──────────────────────
            if msg_type == "update_profile":
                nickname = data.get("nickname", "")
                avatar_id = data.get("avatar_id", "")
                if redis_client:
                    await redis_client.hset(f"profile:{my_uid}", mapping={
                        "nickname": nickname, 
                        "avatar_id": avatar_id
                    })
                    await _send_to(websocket, {"type": "profile_updated", "status": "success"})
                continue

            if msg_type == "get_profile":
                target_uid = data.get("target_uid")
                if redis_client and target_uid:
                    prof = await redis_client.hgetall(f"profile:{target_uid}")
                    is_online = target_uid in active_connections
                    last_seen = await redis_client.get(f"last_seen:{target_uid}")
                    
                    await _send_to(websocket, {
                        "type": "profile_response",
                        "uid": target_uid,
                        "nickname": prof.get("nickname", target_uid),
                        "avatar_id": prof.get("avatar_id", ""),
                        "status": "online" if is_online else "offline",
                        "last_seen": int(last_seen) if last_seen else 0
                    })
                continue

            if msg_type == "check_statuses":
                uids = data.get("uids", [])
                if redis_client and isinstance(uids, list):
                    for u in uids:
                        is_online = u in active_connections
                        last_seen = await redis_client.get(f"last_seen:{u}")
                        await _send_to(websocket, {
                            "type": "user_status",
                            "uid": u,
                            "status": "online" if is_online else "offline",
                            "last_seen": int(last_seen) if last_seen else 0
                        })
                continue

            # ── ОСТАЛЬНЫЕ БАЗОВЫЕ МЕТОДЫ ──────────────────────────────────
            if msg_type == "request_offline_messages":
                target_from_uid = data.get("target_uid") or data.get("from_uid")
                if target_from_uid:
                    await _send_offline_messages_from(websocket, my_uid, target_from_uid)
                continue

            if msg_type == "register_fcm_token":
                token = data.get("fcm_token")
                if redis_client and token:
                    await redis_client.set(f"fcm_token:{my_uid}", token)
                continue

            if msg_type == "register_public_key":
                x25519_key = data.get("x25519_key")
                ed25519_key = data.get("ed25519_key")
                if redis_client and x25519_key and ed25519_key:
                    await redis_client.setex(f"pubkey:{my_uid}:x25519",  30 * 24 * 3600, x25519_key)
                    await redis_client.setex(f"pubkey:{my_uid}:ed25519", 30 * 24 * 3600, ed25519_key)
                continue

            if msg_type == "request_public_key":
                target_uid = data.get("target_uid")
                if redis_client and target_uid:
                    x25519_key  = await redis_client.get(f"pubkey:{target_uid}:x25519")
                    ed25519_key = await redis_client.get(f"pubkey:{target_uid}:ed25519")
                    if x25519_key and ed25519_key:
                        await _send_to(websocket, {
                            "type": "public_key_response", "target_uid": target_uid,
                            "x25519_key": x25519_key, "ed25519_key": ed25519_key,
                        })
                continue

            # ── СООБЩЕНИЯ ─────────────────────────────────────────────────
            if msg_type == "message":
                if not _check_rate_limit(my_uid): continue
                target_uid = data.get("target_uid")
                message_id = data.get("id")
                if not target_uid or not message_id: continue

                payload = {
                    "type":          "message",
                    "from_uid":      my_uid,
                    "id":            message_id,
                    "encrypted_text": data.get("encrypted_text"),
                    "signature":     data.get("signature"),
                    "time":          _now_ms(),
                    "replyToId":     data.get("replyToId"),
                    "messageType":   data.get("messageType", "text"),
                    "mediaData":     data.get("mediaData"),
                    "fileName":      data.get("fileName"),
                    "fileSize":      data.get("fileSize"),
                    "mimeType":      data.get("mimeType"),
                }

                delivered = await _deliver_or_store(target_uid, payload, "new_message", my_uid)
                await _send_to(websocket, {"type": "server_ack", "id": message_id, "delivered_online": delivered})
                continue

            if msg_type == "delete_message":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                if target_uid and message_id:
                    payload = {"type": "message_deleted", "from_uid": my_uid, "message_id": message_id, "time": _now_ms()}
                    await _deliver_or_store(target_uid, payload, "message_deleted", my_uid)
                continue

            if msg_type == "edit_message":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                if target_uid and message_id:
                    payload = {
                        "type": "message_edited", "from_uid": my_uid, "message_id": message_id,
                        "new_encrypted_text": data.get("new_encrypted_text"),
                        "new_signature": data.get("new_signature"), "time": _now_ms()
                    }
                    await _deliver_or_store(target_uid, payload, "message_edited", my_uid)
                continue

            if msg_type == "message_reaction":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                if target_uid and message_id:
                    payload = {
                        "type": "message_reaction", "from_uid": my_uid, "message_id": message_id,
                        "emoji": data.get("emoji"), "action": data.get("action"), "time": _now_ms()
                    }
                    await _deliver_or_store(target_uid, payload, "message_reaction", my_uid)
                continue

            if msg_type == "read_receipt" or msg_type == "delivery_receipt":
                target_uid = data.get("target_uid")
                if target_uid and target_uid in active_connections:
                     await _send_to(active_connections[target_uid], {
                         "type": msg_type, "from_uid": my_uid,
                         "message_id": data.get("message_id"), "time": _now_ms()
                     })
                continue

            if msg_type == "typing_indicator":
                target_uid = data.get("target_uid")
                if target_uid and target_uid in active_connections:
                    await _send_to(active_connections[target_uid], {
                        "type": "typing_indicator", "from_uid": my_uid, "typing": data.get("typing", False)
                    })
                continue

            if msg_type == "ping":
                await _send_to(websocket, {"type": "pong"})
                continue

    except WebSocketDisconnect:
        if my_uid:
            if my_uid in active_connections and active_connections[my_uid] == websocket:
                active_connections.pop(my_uid, None)
                await _update_last_seen(my_uid) # Сохраняем время выхода
            _clean_rate_limit(my_uid)
    except Exception as e:
        if my_uid and my_uid in active_connections and active_connections[my_uid] == websocket:
            active_connections.pop(my_uid, None)
            await _update_last_seen(my_uid)
            _clean_rate_limit(my_uid)
