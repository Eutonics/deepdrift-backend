import asyncio
import base64
import json
import logging
import os
import re
import secrets
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
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.exceptions import InvalidSignature

# ─── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DDChatRelay")

# ─── Приложение ─────────────────────────────────────────────────────────────
app = FastAPI(title="DeepDrift Secure Relay", version="5.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Конфигурация ───────────────────────────────────────────────────────────
REDIS_URL = os.environ.get("REDIS_URL")
FB_JSON   = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

UID_PATTERN = re.compile(r"^\d{6}$")  # UID — строго 6 цифр

# ─── Директория для файлов (Render ephemeral — для S3 замени позже) ─────────
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ─── Auth константы ─────────────────────────────────────────────────────────
NONCE_TTL_SECONDS = 60    # nonce живёт 60 секунд
NONCE_SIZE_BYTES  = 32    # 256 бит случайности

# ─── Глобальное состояние ───────────────────────────────────────────────────
active_connections: Dict[str, WebSocket] = {}
redis_client: Optional[redis.Redis]      = None

_rate_limit: Dict[str, list] = {}
RATE_LIMIT_MAX    = 60
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
    _rate_limit.pop(uid, None)

async def _update_last_seen(uid: str):
    if redis_client:
        try:
            await redis_client.set(f"last_seen:{uid}", _now_ms())
        except Exception:
            pass

# ─── Отправка в сокет ───────────────────────────────────────────────────────
async def _send_to(ws: WebSocket, payload: dict) -> bool:
    try:
        if ws.client_state.name != "CONNECTED":
            return False
        await ws.send_text(json.dumps(payload))
        return True
    except Exception as e:
        logger.warning(f"⚠️ Socket write error: {e}")
        return False

# ─── Push уведомления ───────────────────────────────────────────────────────
async def _send_fcm_push(target_uid: str, from_uid: str, message_type: str = "new_message", group_id: str = None):
    if not redis_client or not firebase_admin._apps:
        return
    try:
        token = await redis_client.get(f"fcm_token:{target_uid}")
        if not token:
            return

        sender_profile = await redis_client.hgetall(f"profile:{from_uid}")
        sender_name = sender_profile.get("nickname", from_uid) if sender_profile else from_uid

        data_payload = {
            "from_uid":    from_uid,
            "sender_name": sender_name,
            "type":        message_type,
            "click_action": "FLUTTER_NOTIFICATION_CLICK",
        }
        if group_id:
            data_payload["target_uid"] = group_id

        msg = messaging.Message(
            data=data_payload,
            token=token,
            android=messaging.AndroidConfig(priority="high"),
            apns=messaging.APNSConfig(
                headers={"apns-priority": "10", "apns-push-type": "background"},
                payload=messaging.APNSPayload(aps=messaging.Aps(content_available=True)),
            ),
        )
        await asyncio.get_event_loop().run_in_executor(None, messaging.send, msg)
        logger.info(f"📲 Push sent to {target_uid} ({message_type})")
    except Exception as e:
        logger.error(f"❌ Push error: {e}")

# ─── Офлайн очереди ─────────────────────────────────────────────────────────
async def _send_offline_messages(websocket: WebSocket, my_uid: str):
    if not redis_client:
        return
    await asyncio.sleep(0.5)
    try:
        offline_key = f"offline_queue:{my_uid}"
        messages = await redis_client.lrange(offline_key, 0, -1)
        if messages:
            logger.info(f"📬 Sending {len(messages)} global offline messages to {my_uid}")
            success_count = 0
            for msg_json in messages:
                if await _send_to(websocket, json.loads(msg_json)):
                    success_count += 1
                else:
                    break
            if success_count > 0:
                await redis_client.ltrim(offline_key, success_count, -1)
    except Exception as e:
        logger.error(f"❌ Error sending offline messages: {e}")

async def _send_offline_messages_from(websocket: WebSocket, my_uid: str, from_uid: str):
    if not redis_client:
        return
    try:
        offline_key = f"offline:{my_uid}:from:{from_uid}"
        messages = await redis_client.lrange(offline_key, 0, -1)
        if messages:
            logger.info(f"📬 Sending {len(messages)} messages from {from_uid} to {my_uid}")
            success_count = 0
            for msg_json in messages:
                if await _send_to(websocket, json.loads(msg_json)):
                    success_count += 1
                else:
                    break
            if success_count > 0:
                await redis_client.ltrim(offline_key, success_count, -1)
    except Exception as e:
        logger.error(f"❌ Error sending specific offline messages: {e}")

async def _store_offline_message(target_uid: str, message_data: dict):
    if not redis_client:
        return
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

# ─── Роутинг ────────────────────────────────────────────────────────────────
async def _deliver_or_store(target_uid: str, payload: dict, push_type: str, from_uid: str, group_id: str = None):
    delivered = False
    if target_uid in active_connections:
        ws = active_connections[target_uid]
        delivered = await _send_to(ws, payload)
        if not delivered:
            logger.warning(f"🔌 Removing dead connection for {target_uid}")
            del active_connections[target_uid]

    if not delivered:
        await _store_offline_message(target_uid, payload)
        await _send_fcm_push(target_uid, from_uid, push_type, group_id)

    return delivered

async def _route_message(target_uid: str, payload: dict, push_type: str, from_uid: str):
    if target_uid.startswith("g_"):
        if redis_client:
            members = await redis_client.smembers(f"group:{target_uid}")
            delivered_any = False
            for member in members:
                if member != from_uid:
                    deliv = await _deliver_or_store(member, payload, push_type, from_uid, group_id=target_uid)
                    if deliv:
                        delivered_any = True
            return delivered_any
        return False
    else:
        return await _deliver_or_store(target_uid, payload, push_type, from_uid)

# ─── AUTH: Challenge-Response через Ed25519 ──────────────────────────────────
#
# Redis-ключи:
#   auth:pubkey:{uid}  →  base64(ed25519_pubkey_32_bytes)   без TTL (постоянно)
#   auth:nonce:{uid}   →  base64(32 random bytes)            TTL 60с
#
# Протокол:
#   init → если pubkey зарегистрирован → auth_challenge → auth_response → uid_assigned
#         если pubkey НЕ зарегистрирован → uid_assigned напрямую (первый запуск)
#   register → сохраняет pubkey (идемпотентно)
# ─────────────────────────────────────────────────────────────────────────────

async def _assign_uid(websocket: WebSocket, uid: str):
    """Финализирует подключение: регистрирует соединение, шлёт uid_assigned."""
    active_connections[uid] = websocket
    await _update_last_seen(uid)
    logger.info(f"✅ {uid} authenticated & connected (total: {len(active_connections)})")
    await _send_to(websocket, {"type": "uid_assigned", "my_uid": uid})
    asyncio.create_task(_send_offline_messages(websocket, uid))


async def _handle_init(websocket: WebSocket, uid_candidate: str) -> Optional[str]:
    """
    Обрабатывает init-сообщение.
    Возвращает uid если аутентификация прошла сразу (нет pubkey → legacy режим).
    Возвращает None если нужен challenge (клиент должен прислать auth_response).
    """
    if not _is_valid_uid(uid_candidate):
        await _send_to(websocket, {"type": "error", "message": "Invalid UID format (6 digits required)"})
        return None

    if not redis_client:
        # Redis недоступен — пускаем без проверки (деградация)
        await _assign_uid(websocket, uid_candidate)
        return uid_candidate

    stored_pubkey = await redis_client.get(f"auth:pubkey:{uid_candidate}")

    if stored_pubkey is None:
        # UID не зарегистрирован → пускаем сразу.
        # Клиент после uid_assigned пришлёт "register" с pubkey.
        await _assign_uid(websocket, uid_candidate)
        return uid_candidate
    else:
        # UID зарегистрирован → требуем подпись нонса
        nonce     = secrets.token_bytes(NONCE_SIZE_BYTES)
        nonce_b64 = base64.b64encode(nonce).decode()
        await redis_client.setex(f"auth:nonce:{uid_candidate}", NONCE_TTL_SECONDS, nonce_b64)
        await _send_to(websocket, {"type": "auth_challenge", "nonce": nonce_b64})
        logger.info(f"🔑 Auth challenge issued for {uid_candidate}")
        return None   # uid не присвоен — ждём auth_response


async def _handle_auth_response(websocket: WebSocket, data: dict) -> Optional[str]:
    """
    Проверяет подпись нонса.
    Возвращает uid при успехе, None при неудаче.
    """
    uid       = str(data.get("uid", "")).strip()
    nonce_b64 = data.get("nonce")
    sig_b64   = data.get("signature")

    if not all([uid, nonce_b64, sig_b64]):
        await _send_to(websocket, {"type": "auth_failed", "reason": "missing_fields"})
        return None

    if not redis_client:
        # Redis недоступен — деградируем, пускаем
        await _assign_uid(websocket, uid)
        return uid

    # Проверяем nonce (должен совпасть и не истечь)
    stored_nonce = await redis_client.get(f"auth:nonce:{uid}")
    if stored_nonce is None:
        await _send_to(websocket, {"type": "auth_failed", "reason": "nonce_expired"})
        logger.warning(f"🚫 Auth failed for {uid}: nonce expired")
        return None
    if stored_nonce != nonce_b64:
        await _send_to(websocket, {"type": "auth_failed", "reason": "nonce_mismatch"})
        logger.warning(f"🚫 Auth failed for {uid}: nonce mismatch")
        return None

    # Nonce одноразовый — удаляем сразу
    await redis_client.delete(f"auth:nonce:{uid}")

    # Получаем зарегистрированный pubkey
    stored_pubkey_b64 = await redis_client.get(f"auth:pubkey:{uid}")
    if stored_pubkey_b64 is None:
        await _send_to(websocket, {"type": "auth_failed", "reason": "not_registered"})
        return None

    # Верифицируем Ed25519-подпись
    try:
        pubkey_bytes = base64.b64decode(stored_pubkey_b64)
        sig_bytes    = base64.b64decode(sig_b64)
        nonce_bytes  = base64.b64decode(nonce_b64)

        pubkey = Ed25519PublicKey.from_public_bytes(pubkey_bytes)
        pubkey.verify(sig_bytes, nonce_bytes)   # raises InvalidSignature if wrong

        await _assign_uid(websocket, uid)
        return uid

    except InvalidSignature:
        await _send_to(websocket, {"type": "auth_failed", "reason": "invalid_signature"})
        logger.warning(f"🚫 Auth failed for {uid}: invalid Ed25519 signature")
        return None
    except Exception as e:
        await _send_to(websocket, {"type": "auth_failed", "reason": f"verification_error"})
        logger.error(f"❌ Auth verification error for {uid}: {e}")
        return None


async def _handle_register(websocket: WebSocket, uid: str, data: dict):
    """
    Привязывает Ed25519 pubkey к uid.
    Идемпотентен: тот же ключ — ок. Другой ключ → uid_taken.
    """
    pubkey_b64 = data.get("ed25519_pubkey")

    if not pubkey_b64:
        await _send_to(websocket, {"type": "error", "reason": "missing ed25519_pubkey"})
        return

    # Валидируем формат pubkey
    try:
        pubkey_bytes = base64.b64decode(pubkey_b64)
        if len(pubkey_bytes) != 32:
            raise ValueError("Ed25519 pubkey must be 32 bytes")
        Ed25519PublicKey.from_public_bytes(pubkey_bytes)
    except Exception as e:
        await _send_to(websocket, {"type": "error", "reason": f"invalid_pubkey: {e}"})
        return

    if not redis_client:
        await _send_to(websocket, {"type": "registered", "uid": uid})
        return

    stored = await redis_client.get(f"auth:pubkey:{uid}")

    if stored is None:
        # Свободный uid — регистрируем
        await redis_client.set(f"auth:pubkey:{uid}", pubkey_b64)
        await _send_to(websocket, {"type": "registered", "uid": uid})
        logger.info(f"📝 New account registered: {uid}")

    elif stored == pubkey_b64:
        # Тот же ключ — idempotent (переустановка приложения / новое устройство с бэкапом)
        await _send_to(websocket, {"type": "registered", "uid": uid})
        logger.info(f"📝 Account re-registered (same key): {uid}")

    else:
        # Uid занят другим ключом
        await _send_to(websocket, {"type": "uid_taken", "reason": "uid already registered with a different key"})
        logger.warning(f"🚫 uid_taken: {uid} tried to register with different pubkey")


# ─── REST endpoints ─────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "status":       "ONLINE",
        "version":      "5.0.0",
        "firebase":     "active" if firebase_admin._apps else "error/disabled",
        "redis":        "connected" if redis_client else "disconnected",
        "users_online": len(active_connections),
    }

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        file_id   = f"{uuid.uuid4().hex}_{file.filename}"
        file_path = os.path.join(UPLOAD_DIR, file_id)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"status": "success", "file_id": file_id}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/download/{file_id}")
async def download_file(file_id: str):
    safe_file_id = os.path.basename(file_id)
    file_path    = os.path.join(UPLOAD_DIR, safe_file_id)
    if os.path.exists(file_path):
        return FileResponse(file_path)
    logger.warning(f"⚠️ HTTP Download failed (not found): {safe_file_id}")
    from fastapi import Response
    return Response(status_code=404, content="File not found")


# ─── WebSocket ──────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    my_uid: Optional[str]           = None   # uid после успешной аутентификации
    pending_auth_uid: Optional[str] = None   # uid ожидающий auth_response

    try:
        while True:
            raw      = await websocket.receive_text()
            data     = json.loads(raw)
            msg_type = data.get("type")

            # ── INIT ──────────────────────────────────────────────────────────
            if msg_type == "init":
                uid_candidate = str(data.get("my_uid", "")).strip()
                result = await _handle_init(websocket, uid_candidate)
                if result:
                    # Аутентификация прошла сразу (нет зарегистрированного pubkey)
                    my_uid = result
                else:
                    # Ждём auth_response — запоминаем uid для следующего шага
                    pending_auth_uid = uid_candidate
                continue

            # ── AUTH_RESPONSE ─────────────────────────────────────────────────
            if msg_type == "auth_response":
                result = await _handle_auth_response(websocket, data)
                if result:
                    my_uid           = result
                    pending_auth_uid = None
                continue

            # ── Всё остальное требует аутентифицированного соединения ─────────
            if not my_uid:
                await _send_to(websocket, {"type": "error", "message": "Not authenticated."})
                continue

            await _update_last_seen(my_uid)

            # ── REGISTER (привязка pubkey к uid) ──────────────────────────────
            if msg_type == "register":
                await _handle_register(websocket, my_uid, data)
                continue

            # ── ГРУППЫ ───────────────────────────────────────────────────────
            if msg_type == "create_group":
                group_id = data.get("group_id")
                members  = data.get("members", [])
                if redis_client and group_id and members:
                    if my_uid not in members:
                        members.append(my_uid)
                    await redis_client.sadd(f"group:{group_id}", *members)
                    await _send_to(websocket, {"type": "group_created", "group_id": group_id})
                continue

            # ── ПРОФИЛЬ ───────────────────────────────────────────────────────
            if msg_type == "update_profile":
                nickname  = data.get("nickname") or ""
                avatar_id = data.get("avatar_id") or ""
                if redis_client:
                    await redis_client.hset(f"profile:{my_uid}", mapping={"nickname": nickname, "avatar_id": avatar_id})
                    await _send_to(websocket, {"type": "profile_updated", "status": "success"})
                continue

            if msg_type == "get_profile":
                target_uid = data.get("target_uid")
                if redis_client and target_uid:
                    prof      = await redis_client.hgetall(f"profile:{target_uid}")
                    is_online = target_uid in active_connections
                    last_seen = await redis_client.get(f"last_seen:{target_uid}")
                    await _send_to(websocket, {
                        "type":      "profile_response",
                        "uid":       target_uid,
                        "nickname":  prof.get("nickname", target_uid),
                        "avatar_id": prof.get("avatar_id", ""),
                        "status":    "online" if is_online else "offline",
                        "last_seen": int(last_seen) if last_seen else 0,
                    })
                continue

            if msg_type == "check_statuses":
                uids = data.get("uids", [])
                if redis_client and isinstance(uids, list):
                    for u in uids:
                        if str(u).startswith("g_"):
                            continue
                        is_online = u in active_connections
                        last_seen = await redis_client.get(f"last_seen:{u}")
                        await _send_to(websocket, {
                            "type":      "user_status",
                            "uid":       u,
                            "status":    "online" if is_online else "offline",
                            "last_seen": int(last_seen) if last_seen else 0,
                        })
                continue

            # ── ОФЛАЙН ОЧЕРЕДЬ ────────────────────────────────────────────────
            if msg_type == "request_offline_messages":
                target_from_uid = data.get("target_uid") or data.get("from_uid")
                if target_from_uid:
                    await _send_offline_messages_from(websocket, my_uid, target_from_uid)
                continue

            # ── FCM TOKEN ────────────────────────────────────────────────────
            if msg_type == "register_fcm_token":
                token = data.get("fcm_token")
                if redis_client and token:
                    await redis_client.set(f"fcm_token:{my_uid}", token)
                continue

            # ── ПУБЛИЧНЫЕ КЛЮЧИ (для E2E шифрования, не для auth) ─────────────
            if msg_type == "register_public_key":
                x25519_key  = data.get("x25519_key")
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
                            "type":        "public_key_response",
                            "target_uid":  target_uid,
                            "x25519_key":  x25519_key,
                            "ed25519_key": ed25519_key,
                        })
                continue

            # ── СООБЩЕНИЯ ─────────────────────────────────────────────────────
            if msg_type == "message":
                if not _check_rate_limit(my_uid):
                    continue
                target_uid = data.get("target_uid")
                message_id = data.get("id")
                if not target_uid or not message_id:
                    continue

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
                }
                payload   = {k: v for k, v in raw_payload.items() if v is not None}
                delivered = await _route_message(target_uid, payload, "new_message", my_uid)
                await _send_to(websocket, {"type": "server_ack", "id": message_id, "delivered_online": delivered})
                continue

            if msg_type == "delete_message":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                if target_uid and message_id:
                    payload = {
                        "type": "message_deleted", "target_uid": target_uid,
                        "from_uid": my_uid, "message_id": message_id, "time": _now_ms(),
                    }
                    await _route_message(target_uid, payload, "message_deleted", my_uid)
                continue

            if msg_type == "edit_message":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                if target_uid and message_id:
                    payload = {
                        "type":             "message_edited",
                        "target_uid":       target_uid,
                        "from_uid":         my_uid,
                        "message_id":       message_id,
                        "new_encrypted_text": data.get("new_encrypted_text"),
                        "new_signature":    data.get("new_signature"),
                        "time":             _now_ms(),
                    }
                    await _route_message(target_uid, payload, "message_edited", my_uid)
                continue

            if msg_type == "message_reaction":
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
                    await _route_message(target_uid, payload, "message_reaction", my_uid)
                continue

            if msg_type in ("read_receipt", "delivery_receipt"):
                target_uid = data.get("target_uid")
                if target_uid and target_uid in active_connections:
                    await _send_to(active_connections[target_uid], {
                        "type":       msg_type,
                        "from_uid":   my_uid,
                        "target_uid": target_uid,
                        "message_id": data.get("message_id"),
                        "time":       _now_ms(),
                    })
                continue

            if msg_type == "typing_indicator":
                target_uid = data.get("target_uid")
                if target_uid and target_uid in active_connections:
                    await _send_to(active_connections[target_uid], {
                        "type":       "typing_indicator",
                        "from_uid":   my_uid,
                        "target_uid": target_uid,
                        "typing":     data.get("typing", False),
                    })
                continue

            if msg_type == "ping":
                await _send_to(websocket, {"type": "pong"})
                continue

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"❌ WebSocket loop error: {e}")
    finally:
        if my_uid:
            if my_uid in active_connections and active_connections[my_uid] == websocket:
                active_connections.pop(my_uid, None)
                await _update_last_seen(my_uid)
                logger.info(f"👋 {my_uid} disconnected (total: {len(active_connections)})")
            _clean_rate_limit(my_uid)
