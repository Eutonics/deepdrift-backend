"""
DeepDrift Secure Relay — v6.0.0
Модульный WebSocket relay-сервер для E2E-шифрованного мессенджера.

Изменения по сравнению с v5.1.0:
- Модульная архитектура (handlers/, services/)
- Redis Pub/Sub для горизонтального масштабирования
- lifespan вместо deprecated on_event
- /health endpoint
- Ограничение размера WS-сообщений
- Пагинация поиска каналов
- Интеграция server_metrics
- Блокировка пользователей (block/unblock)
- delete_group, demote_admin
- FCM token rotation (обработка UnregisteredError)
- Rate limit уведомление пользователю
- Безопасный _validate_upload_token (False при недоступном Redis)
- Защита от утечки памяти в fallback rate limiter
"""
import asyncio
import io
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, UploadFile, File
from fastapi.responses import JSONResponse, Response, StreamingResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware

from config import (
    APP_TITLE, APP_VERSION, CORS_ORIGINS,
    MAX_UPLOAD_SIZE, WS_MAX_MESSAGE_SIZE, UPLOAD_DIR,
)
from services import (
    RedisService, StorageBackend, PushService,
    RateLimiter, OfflineQueue, ConnectionManager,
)
from handlers import AuthHandler, MessageHandler, GroupHandler, ChannelHandler, ProfileHandler
from server_metrics import (
    init_metrics, track_connection, track_disconnection,
    track_error, track_rate_limit, MessageTimer, ConnectionTimer,
)

# ─── Логирование ────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DDChatRelay")

# ─── Сервисы (синглтоны) ────────────────────────────────────────────────────
redis_svc   = RedisService()
storage_svc = StorageBackend()
push_svc    = PushService()
rate_limiter = RateLimiter()
conn_mgr    = ConnectionManager()
offline_q   = OfflineQueue(conn_mgr)

# Связываем connection manager с Redis для Pub/Sub
conn_mgr.set_redis(redis_svc)

# ─── Хендлеры ───────────────────────────────────────────────────────────────
auth_h    = AuthHandler(conn_mgr, offline_q)
msg_h     = MessageHandler(conn_mgr, offline_q, push_svc, rate_limiter)
group_h   = GroupHandler(conn_mgr, offline_q, push_svc)
channel_h = ChannelHandler(conn_mgr)
profile_h = ProfileHandler(conn_mgr)


# ─── Lifespan (замена deprecated on_event) ──────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await redis_svc.connect()
    if redis_svc.available:
        await redis_svc.start_pubsub(conn_mgr.handle_pubsub_message)
    yield
    # Shutdown
    await redis_svc.disconnect()


# ─── Приложение ─────────────────────────────────────────────────────────────
app = FastAPI(title=APP_TITLE, version=APP_VERSION, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Интегрируем Prometheus метрики
init_metrics(app)


# ─── Хелперы ────────────────────────────────────────────────────────────────
def _now_ms() -> int:
    return int(datetime.now().timestamp() * 1000)


async def _update_last_seen(uid: str):
    rc = redis_svc.client
    if rc:
        try:
            await rc.set(f"last_seen:{uid}", _now_ms())
        except Exception:
            pass


# ─── REST endpoints ─────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "status":       "ONLINE",
        "version":      APP_VERSION,
        "firebase":     "active" if push_svc.available else "disabled",
        "redis":        "connected" if redis_svc.available else "disconnected",
        "storage":      storage_svc.storage_type,
        "users_online": conn_mgr.online_count,
    }


@app.get("/health")
async def health():
    """Health-check для Docker/Render/K8s."""
    checks = {
        "redis": redis_svc.available,
        "firebase": push_svc.available,
    }
    healthy = checks["redis"]  # Redis — критический сервис
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "healthy" if healthy else "degraded", "checks": checks},
    )


@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...)):
    token = request.headers.get("x-upload-token") or request.headers.get("X-Upload-Token")
    if not await auth_h.validate_upload_token(token, redis_svc.client):
        return JSONResponse(status_code=401, content={"status": "error", "message": "Unauthorized"})

    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_UPLOAD_SIZE:
        return JSONResponse(status_code=413, content={"status": "error", "message": "File too large (max 150 MB)"})

    try:
        file_data = await file.read()
        if len(file_data) > MAX_UPLOAD_SIZE:
            return JSONResponse(status_code=413, content={"status": "error", "message": "File too large (max 150 MB)"})

        file_id = await storage_svc.upload(file_data, file.filename, file.content_type)
        return {"status": "success", "file_id": file_id}
    except Exception as e:
        logger.error(f"❌ Upload error: {e}")
        track_error("upload")
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})


@app.get("/download/{file_id}")
async def download_file(file_id: str, request: Request, token: str = None):
    auth_token = token or request.headers.get("x-upload-token") or request.headers.get("X-Upload-Token")
    if not await auth_h.validate_upload_token(auth_token, redis_svc.client):
        return Response(status_code=401, content="Unauthorized")

    try:
        result = await storage_svc.download(file_id)
        if result is None:
            return Response(status_code=404, content="File not found")
        body, content_type = result
        safe_file_id = os.path.basename(file_id)
        return StreamingResponse(
            io.BytesIO(body),
            media_type=content_type,
            headers={"Content-Disposition": f"attachment; filename={safe_file_id}"},
        )
    except Exception as e:
        logger.error(f"❌ Download error: {e}")
        track_error("download")
        return Response(status_code=500, content="Storage error")


# ─── WebSocket ──────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    track_connection()

    my_uid: Optional[str]          = None
    pending_auth_uid: Optional[str] = None
    rc = redis_svc.client  # shorthand

    try:
        with ConnectionTimer():
            while True:
                raw = await websocket.receive_text()

                # ── Ограничение размера сообщения ────────────────────────────
                if len(raw) > WS_MAX_MESSAGE_SIZE:
                    await conn_mgr.send_to(websocket, {
                        "type": "error", "message": "Message too large",
                    })
                    track_error("message_too_large")
                    continue

                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    track_error("json_decode")
                    continue

                msg_type = data.get("type")

                # ── INIT ─────────────────────────────────────────────────────
                if msg_type == "init":
                    uid_candidate = str(data.get("my_uid", "")).strip()
                    result = await auth_h.handle_init(websocket, uid_candidate, rc)
                    if result:
                        my_uid = result
                    else:
                        pending_auth_uid = uid_candidate
                    continue

                # ── AUTH_RESPONSE ────────────────────────────────────────────
                if msg_type == "auth_response":
                    result = await auth_h.handle_auth_response(websocket, data, rc)
                    if result:
                        my_uid           = result
                        pending_auth_uid = None
                    continue

                # ── Всё остальное требует аутентификации ─────────────────────
                if not my_uid:
                    await conn_mgr.send_to(websocket, {"type": "error", "message": "Not authenticated."})
                    continue

                await _update_last_seen(my_uid)

                # ── REGISTER ─────────────────────────────────────────────────
                if msg_type == "register":
                    await auth_h.handle_register(websocket, my_uid, data, rc)
                    continue

                # ── ПРОФИЛЬ ──────────────────────────────────────────────────
                if msg_type == "update_profile":
                    await profile_h.handle_update_profile(websocket, my_uid, data, rc)
                    continue
                if msg_type == "get_profile":
                    await profile_h.handle_get_profile(websocket, my_uid, data, rc)
                    continue
                if msg_type == "check_statuses":
                    await profile_h.handle_check_statuses(websocket, my_uid, data, rc)
                    continue

                # ── ПУБЛИЧНЫЕ КЛЮЧИ ──────────────────────────────────────────
                if msg_type == "register_public_key":
                    await profile_h.handle_register_public_key(my_uid, data, rc)
                    continue
                if msg_type == "request_public_key":
                    await profile_h.handle_request_public_key(websocket, data, rc)
                    continue

                # ── FCM TOKEN ────────────────────────────────────────────────
                if msg_type == "register_fcm_token":
                    await profile_h.handle_register_fcm(my_uid, data, rc)
                    continue

                # ── ОФЛАЙН ОЧЕРЕДЬ ───────────────────────────────────────────
                if msg_type == "request_offline_messages":
                    target_from_uid = data.get("target_uid") or data.get("from_uid")
                    if target_from_uid:
                        await offline_q.send_from(websocket, my_uid, target_from_uid, rc)
                    continue

                # ── БЛОКИРОВКА ───────────────────────────────────────────────
                if msg_type == "block_user":
                    await profile_h.handle_block_user(websocket, my_uid, data, rc)
                    continue
                if msg_type == "unblock_user":
                    await profile_h.handle_unblock_user(websocket, my_uid, data, rc)
                    continue
                if msg_type == "get_blocked":
                    await profile_h.handle_get_blocked(websocket, my_uid, rc)
                    continue

                # ── СООБЩЕНИЯ ────────────────────────────────────────────────
                if msg_type == "message":
                    # Проверяем блокировку: если получатель заблокировал отправителя
                    target_uid = data.get("target_uid")
                    if target_uid and not target_uid.startswith("g_"):
                        if await profile_h.is_blocked(rc, target_uid, my_uid):
                            await conn_mgr.send_to(websocket, {
                                "type": "error", "message": "You are blocked by this user",
                            })
                            continue
                    with MessageTimer():
                        await msg_h.handle_message(websocket, my_uid, data, rc)
                    continue

                if msg_type == "delete_message":
                    await msg_h.handle_delete(my_uid, data, rc)
                    continue
                if msg_type == "edit_message":
                    await msg_h.handle_edit(my_uid, data, rc)
                    continue
                if msg_type == "message_reaction":
                    await msg_h.handle_reaction(my_uid, data, rc)
                    continue
                if msg_type in ("read_receipt", "delivery_receipt"):
                    await msg_h.handle_receipt(my_uid, msg_type, data)
                    continue
                if msg_type == "typing_indicator":
                    await msg_h.handle_typing(my_uid, data)
                    continue

                # ── ГРУППЫ ───────────────────────────────────────────────────
                if msg_type == "create_group":
                    await group_h.handle_create(websocket, my_uid, data, rc)
                    continue
                if msg_type == "leave_group":
                    await group_h.handle_leave(websocket, my_uid, data, rc)
                    continue
                if msg_type == "kick_member":
                    await group_h.handle_kick(websocket, my_uid, data, rc)
                    continue
                if msg_type == "promote_admin":
                    await group_h.handle_promote(websocket, my_uid, data, rc)
                    continue
                if msg_type == "demote_admin":
                    await group_h.handle_demote(websocket, my_uid, data, rc)
                    continue
                if msg_type == "delete_group":
                    await group_h.handle_delete_group(websocket, my_uid, data, rc)
                    continue
                if msg_type == "update_group_settings":
                    await group_h.handle_update_settings(websocket, my_uid, data, rc)
                    continue
                if msg_type == "distribute_group_keys":
                    await group_h.handle_distribute_keys(my_uid, data, rc)
                    continue
                if msg_type == "get_group_key":
                    await group_h.handle_get_key(websocket, my_uid, data, rc)
                    continue

                # ── КАНАЛЫ ───────────────────────────────────────────────────
                if msg_type == "create_channel":
                    await channel_h.handle_create(websocket, my_uid, data, rc)
                    continue
                if msg_type == "search_channels":
                    await channel_h.handle_search(websocket, data, rc)
                    continue
                if msg_type == "join_channel":
                    await channel_h.handle_join(websocket, my_uid, data, rc)
                    continue
                if msg_type == "leave_channel":
                    await channel_h.handle_leave(websocket, my_uid, data, rc)
                    continue
                if msg_type == "channel_message":
                    await channel_h.handle_message(websocket, my_uid, data, rc)
                    continue
                if msg_type == "delete_channel":
                    await channel_h.handle_delete(websocket, my_uid, data, rc)
                    continue

                # ── PING ─────────────────────────────────────────────────────
                if msg_type == "ping":
                    await conn_mgr.send_to(websocket, {"type": "pong"})
                    continue

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"❌ WebSocket loop error: {e}")
        track_error("websocket_loop")
    finally:
        track_disconnection()
        if my_uid:
            await conn_mgr.unregister(my_uid, websocket)
            await _update_last_seen(my_uid)
            logger.info(f"👋 {my_uid} disconnected (total: {conn_mgr.online_count})")
            rate_limiter.clean(my_uid)
