import asyncio
import base64
import json
import logging
import os
import re
import secrets
import time
import uuid
from datetime import datetime
from typing import Dict, Optional

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import redis.asyncio as redis
import firebase_admin
from firebase_admin import credentials, messaging
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from cryptography.exceptions import InvalidSignature

# ─── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DDChatRelay")

# ─── Приложение ─────────────────────────────────────────────────────────────
app = FastAPI(title="DeepDrift Secure Relay", version="5.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ─── Конфигурация ───────────────────────────────────────────────────────────
REDIS_URL = os.environ.get("REDIS_URL")
FB_JSON   = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

UID_PATTERN = re.compile(r"^\d{6}$")  # UID — строго 6 цифр

# ─── Cloudflare R2 (S3-совместимое постоянное хранилище) ────────────────────
# Переменные окружения (добавь в Render → Environment):
#   R2_ENDPOINT_URL    — https://<account_id>.r2.cloudflarestorage.com
#   R2_ACCESS_KEY_ID   — Access Key ID из API Token
#   R2_SECRET_KEY      — Secret Access Key из API Token
#   R2_BUCKET_NAME     — имя bucket (напр. ddchat-files)
R2_ENDPOINT  = os.environ.get("R2_ENDPOINT_URL", "")
R2_KEY_ID    = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET    = os.environ.get("R2_SECRET_KEY", "")
R2_BUCKET    = os.environ.get("R2_BUCKET_NAME", "ddchat-files")

# Fallback: локальная папка если R2 не настроен (не пропадёт при рестарте, но и не надёжно)
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def _get_r2_client():
    """Создаёт boto3 S3-клиент для Cloudflare R2. Возвращает None если не настроен."""
    if not all([R2_ENDPOINT, R2_KEY_ID, R2_SECRET]):
        return None
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_KEY_ID,
        aws_secret_access_key=R2_SECRET,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        ),
        region_name="auto",
    )

# Инициализируем клиент при старте
_r2 = _get_r2_client()
if _r2:
    logger.info(f"✅ R2 storage configured: bucket={R2_BUCKET}")
else:
    logger.warning("⚠️ R2 not configured — using local disk (ephemeral on Render!)")

# ─── Auth константы ─────────────────────────────────────────────────────────
NONCE_TTL_SECONDS = 60    # nonce живёт 60 секунд
NONCE_SIZE_BYTES  = 32    # 256 бит случайности

# ─── Глобальное состояние ───────────────────────────────────────────────────
active_connections: Dict[str, WebSocket] = {}
redis_client: Optional[redis.Redis]      = None

_rate_limit: Dict[str, list] = {}   # fallback when Redis unavailable
RATE_LIMIT_MAX    = 60
RATE_LIMIT_WINDOW = 60

# Лимит размера загружаемого файла: 150 МБ
MAX_UPLOAD_SIZE = 150 * 1024 * 1024

# TTL токена загрузки (привязан к сессии WebSocket)
UPLOAD_TOKEN_TTL = 24 * 3600  # 24 часа

# asyncio.Lock для atomic операций с active_connections
_connections_lock = asyncio.Lock()

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

def _check_rate_limit_memory(uid: str) -> bool:
    """Fallback rate limit — используется только когда Redis недоступен."""
    now = datetime.now().timestamp()
    timestamps = _rate_limit.get(uid, [])
    timestamps = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
    if len(timestamps) >= RATE_LIMIT_MAX:
        return False
    timestamps.append(now)
    _rate_limit[uid] = timestamps
    return True

async def _check_rate_limit(uid: str) -> bool:
    """Redis sliding-window rate limit (60 сообщений / 60 секунд).
    Устойчив к горизонтальному масштабированию и рестарту сервера."""
    if not redis_client:
        return _check_rate_limit_memory(uid)
    try:
        now    = time.time()
        key    = f"rate:{uid}"
        cutoff = now - RATE_LIMIT_WINDOW
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.zremrangebyscore(key, 0, cutoff)
            pipe.zadd(key, {str(now): now})
            pipe.zcard(key)
            pipe.expire(key, RATE_LIMIT_WINDOW)
            results = await pipe.execute()
        count = results[2]
        return count <= RATE_LIMIT_MAX
    except Exception:
        return _check_rate_limit_memory(uid)

def _clean_rate_limit(uid: str):
    _rate_limit.pop(uid, None)

async def _validate_upload_token(token: str | None) -> bool:
    """Проверяет upload_token — выдаётся при подключении WebSocket.
    Если Redis недоступен или упал — пропускаем (деградация безопасна)."""
    if not redis_client:
        return True   # без Redis не можем проверить — разрешаем
    if not token:
        return False
    try:
        uid = await redis_client.get(f"upload_token:{token}")
        return uid is not None
    except Exception:
        return True   # Redis недоступен в момент запроса — не блокируем пользователя

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
        from_uid   = message_data.get("from_uid", "unknown")
        message_id = message_data.get("id")
        ttl        = 7 * 24 * 3600  # 7 дней

        # ── Дедупликация по message_id ────────────────────────────────────────
        # Предотвращает двойную очередь при повторных вызовах (сетевые ретраи и т.п.)
        if message_id:
            dedup_key = f"offline_id:{target_uid}:{message_id}"
            if await redis_client.exists(dedup_key):
                logger.debug(f"🔁 Dedup: skipping already-queued msg {message_id} for {target_uid}")
                return
            await redis_client.setex(dedup_key, ttl, "1")

        offline_key_global = f"offline_queue:{target_uid}"
        await redis_client.rpush(offline_key_global, json.dumps(message_data))
        await redis_client.expire(offline_key_global, ttl)

        offline_key_specific = f"offline:{target_uid}:from:{from_uid}"
        await redis_client.rpush(offline_key_specific, json.dumps(message_data))
        await redis_client.expire(offline_key_specific, ttl)
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
    async with _connections_lock:
        active_connections[uid] = websocket
    await _update_last_seen(uid)
    logger.info(f"✅ {uid} authenticated & connected (total: {len(active_connections)})")

    # Генерируем upload_token — клиент использует его для авторизации /upload и /download
    upload_token = secrets.token_urlsafe(32)
    if redis_client:
        try:
            await redis_client.setex(f"upload_token:{upload_token}", UPLOAD_TOKEN_TTL, uid)
        except Exception:
            pass

    await _send_to(websocket, {
        "type":         "uid_assigned",
        "my_uid":       uid,
        "upload_token": upload_token,
    })
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
        "version":      "5.1.0",
        "firebase":     "active" if firebase_admin._apps else "error/disabled",
        "redis":        "connected" if redis_client else "disconnected",
        "storage":      f"r2:{R2_BUCKET}" if _r2 else "local_disk (ephemeral!)",
        "users_online": len(active_connections),
    }

@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    # ── Авторизация ────────────────────────────────────────────────────────
    token = request.headers.get("x-upload-token") or request.headers.get("X-Upload-Token")
    if not await _validate_upload_token(token):
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=401, content={"status": "error", "message": "Unauthorized"})

    # ── Лимит размера: 150 МБ ─────────────────────────────────────────────
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_UPLOAD_SIZE:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=413,
            content={"status": "error", "message": "File too large (max 150 MB)"},
        )

    try:
        # Безопасное имя: uuid + оригинальное имя без директорий
        safe_name = os.path.basename(file.filename or "file")
        file_id   = f"{uuid.uuid4().hex}_{safe_name}"
        file_data = await file.read()

        # Проверяем фактический размер после чтения (на случай отсутствия Content-Length)
        if len(file_data) > MAX_UPLOAD_SIZE:
            from fastapi.responses import JSONResponse
            return JSONResponse(
                status_code=413,
                content={"status": "error", "message": "File too large (max 150 MB)"},
            )

        if _r2:
            # ── Загружаем в Cloudflare R2 ────────────────────────────────────
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: _r2.put_object(
                    Bucket=R2_BUCKET,
                    Key=file_id,
                    Body=file_data,
                    ContentType=file.content_type or "application/octet-stream",
                ),
            )
            logger.info(f"📦 Uploaded to R2: {file_id} ({len(file_data)} bytes)")
        else:
            # ── Fallback: локальный диск ──────────────────────────────────────
            file_path = os.path.join(UPLOAD_DIR, file_id)
            with open(file_path, "wb") as f:
                f.write(file_data)
            logger.info(f"💾 Saved locally: {file_id} ({len(file_data)} bytes)")

        return {"status": "success", "file_id": file_id}
    except Exception as e:
        logger.error(f"❌ Upload error: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/download/{file_id}")
async def download_file(file_id: str, request: Request, token: str = None):
    from fastapi import Response
    from fastapi.responses import StreamingResponse
    import io

    # Принимаем токен из query param (?token=...) или заголовка
    auth_token = token or request.headers.get("x-upload-token") or request.headers.get("X-Upload-Token")
    if not await _validate_upload_token(auth_token):
        return Response(status_code=401, content="Unauthorized")

    safe_file_id = os.path.basename(file_id)

    if _r2:
        # ── Отдаём из Cloudflare R2 ──────────────────────────────────────────
        try:
            obj = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: _r2.get_object(Bucket=R2_BUCKET, Key=safe_file_id),
            )
            body         = obj["Body"].read()
            content_type = obj.get("ContentType", "application/octet-stream")
            return StreamingResponse(
                io.BytesIO(body),
                media_type=content_type,
                headers={"Content-Disposition": f"attachment; filename={safe_file_id}"},
            )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("NoSuchKey", "404"):
                logger.warning(f"⚠️ R2 Download not found: {safe_file_id}")
                return Response(status_code=404, content="File not found")
            logger.error(f"❌ R2 Download error: {e}")
            return Response(status_code=500, content="Storage error")
    else:
        # ── Fallback: локальный диск ─────────────────────────────────────────
        file_path = os.path.join(UPLOAD_DIR, safe_file_id)
        if os.path.exists(file_path):
            return FileResponse(file_path)
        logger.warning(f"⚠️ Local Download not found: {safe_file_id}")
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
                group_id   = data.get("group_id")
                members    = data.get("members", [])
                group_name = data.get("group_name", group_id)
                if redis_client and group_id and members:
                    if my_uid not in members:
                        members.append(my_uid)
                    await redis_client.sadd(f"group:{group_id}", *members)
                    await redis_client.set(f"group_name:{group_id}", group_name)
                    # Создатель — администратор группы
                    await redis_client.sadd(f"group_admins:{group_id}", my_uid)
                    await _send_to(websocket, {"type": "group_created", "group_id": group_id})
                    # Уведомляем всех участников (включая офлайн — через очередь)
                    invite = {
                        "type":       "group_invited",
                        "group_id":   group_id,
                        "group_name": group_name,
                        "creator_uid": my_uid,
                        "from_uid":   my_uid,
                        "members":    members,
                    }
                    for member_uid in members:
                        if member_uid == my_uid:
                            continue
                        await _route_message(member_uid, invite, "group_invited", my_uid)
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
                    # Если это группа — возвращаем данные группы
                    if str(target_uid).startswith("g_"):
                        group_name = await redis_client.get(f"group_name:{target_uid}") or target_uid
                        members    = list(await redis_client.smembers(f"group:{target_uid}"))
                        admins     = list(await redis_client.smembers(f"group_admins:{target_uid}"))
                        await _send_to(websocket, {
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
                if not await _check_rate_limit(my_uid):
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
                    "group_id":       data.get("group_id"),
                }
                payload = {k: v for k, v in raw_payload.items() if v is not None}

                # Для группы: fan-out всем участникам через _route_message.
                # Групповое сообщение зашифровано симметричным ключом группы (один payload).
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

            # ── ГРУППОВЫЕ КЛЮЧИ ───────────────────────────────────────────────────
            # Создатель шлёт зашифрованные копии группового ключа для каждого участника.
            # Сервер хранит их в Redis; каждый участник получает только свою копию.
            if msg_type == "distribute_group_keys":
                group_id      = data.get("group_id")
                encrypted_keys = data.get("encrypted_keys", {})  # {uid: encryptedBlob}
                if redis_client and group_id and encrypted_keys:
                    KEY_TTL = 90 * 24 * 3600  # 90 дней
                    for uid, blob in encrypted_keys.items():
                        key = f"group_key:{group_id}:{uid}"
                        await redis_client.setex(key, KEY_TTL, json.dumps({
                            "blob":    blob,
                            "creator": my_uid,
                        }))
                    logger.info(f"🔑 Group keys stored for {group_id} ({len(encrypted_keys)} members)")
                continue

            if msg_type == "get_group_key":
                group_id = data.get("group_id")
                if redis_client and group_id:
                    raw = await redis_client.get(f"group_key:{group_id}:{my_uid}")
                    if raw:
                        entry = json.loads(raw)
                        await _send_to(websocket, {
                            "type":          "group_key_response",
                            "group_id":      group_id,
                            "encrypted_key": entry.get("blob"),
                            "creator_uid":   entry.get("creator"),
                        })
                    else:
                        await _send_to(websocket, {
                            "type":     "group_key_not_found",
                            "group_id": group_id,
                        })
                continue

            if msg_type == "leave_group":
                group_id = data.get("group_id")
                if redis_client and group_id:
                    await redis_client.srem(f"group:{group_id}", my_uid)
                    # Удаляем групповой ключ этого пользователя
                    await redis_client.delete(f"group_key:{group_id}:{my_uid}")
                    # Уведомляем оставшихся участников
                    members = await redis_client.smembers(f"group:{group_id}")
                    leave_msg = {
                        "type":     "group_member_left",
                        "group_id": group_id,
                        "uid":      my_uid,
                        "time":     _now_ms(),
                    }
                    for member in members:
                        if member != my_uid and member in active_connections:
                            await _send_to(active_connections[member], leave_msg)
                    logger.info(f"👋 {my_uid} left group {group_id}")
                continue

            if msg_type == "kick_member":
                # Выгнать участника — только admin может
                group_id   = data.get("group_id")
                target_uid = data.get("target_uid")
                if redis_client and group_id and target_uid:
                    admins = await redis_client.smembers(f"group_admins:{group_id}")
                    if my_uid not in admins:
                        await _send_to(websocket, {"type": "error", "message": "Not an admin"})
                        continue
                    await redis_client.srem(f"group:{group_id}", target_uid)
                    await redis_client.delete(f"group_key:{group_id}:{target_uid}")
                    # Уведомляем всех
                    members = await redis_client.smembers(f"group:{group_id}")
                    kick_msg = {
                        "type":       "group_member_kicked",
                        "group_id":   group_id,
                        "uid":        target_uid,
                        "by_uid":     my_uid,
                        "time":       _now_ms(),
                    }
                    if target_uid in active_connections:
                        await _send_to(active_connections[target_uid], kick_msg)
                    for member in members:
                        if member in active_connections:
                            await _send_to(active_connections[member], kick_msg)
                    logger.info(f"🦵 {my_uid} kicked {target_uid} from group {group_id}")
                continue

            if msg_type == "promote_admin":
                # Назначить участника администратором — только admin
                group_id   = data.get("group_id")
                target_uid = data.get("target_uid")
                if redis_client and group_id and target_uid:
                    admins = await redis_client.smembers(f"group_admins:{group_id}")
                    if my_uid not in admins:
                        await _send_to(websocket, {"type": "error", "message": "Not an admin"})
                        continue
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
                        if member in active_connections:
                            await _send_to(active_connections[member], notify)
                    logger.info(f"⭐ {my_uid} promoted {target_uid} in group {group_id}")
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
            async with _connections_lock:
                if my_uid in active_connections and active_connections[my_uid] == websocket:
                    active_connections.pop(my_uid, None)
            await _update_last_seen(my_uid)
            logger.info(f"👋 {my_uid} disconnected (total: {len(active_connections)})")
            _clean_rate_limit(my_uid)
