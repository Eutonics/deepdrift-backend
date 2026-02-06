from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import asyncio
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import redis.asyncio as redis
from collections import defaultdict
import secrets
import firebase_admin
from firebase_admin import credentials, messaging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("DDChatRelay")

app = FastAPI(title="DDChat Secure Relay", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION FROM RENDER ---
REDIS_URL = os.environ.get("REDIS_URL")
FB_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

# In-memory Fallbacks
active_connections: Dict[str, WebSocket] = {}
user_tokens: Dict[str, str] = {}
message_counts = defaultdict(list)
redis_client: Optional[redis.Redis] = None

# Константы
MAX_MESSAGES_PER_MINUTE = 60
OFFLINE_MESSAGE_TTL = 604800  # 7 дней
PROTOCOL_VERSION = "2.0"

# --- ИНИЦИАЛИЗАЦИЯ FIREBASE V1 ---
try:
    if FB_JSON:
        fb_dict = json.loads(FB_JSON)
        cred = credentials.Certificate(fb_dict)
        firebase_admin.initialize_app(cred)
        logger.info("✅ Firebase Admin SDK initialized (V1)")
    else:
        logger.warning("⚠️ FIREBASE_SERVICE_ACCOUNT_JSON not set in Render")
except Exception as e:
    logger.error(f"❌ Firebase Init Error: {e}")

async def init_redis():
    global redis_client
    if REDIS_URL:
        try:
            # Исправляем протокол для Render
            url = REDIS_URL.replace("cache://", "redis://")
            redis_client = await redis.from_url(
                url, encoding="utf-8", decode_responses=True, ssl_cert_reqs=None
            )
            await redis_client.ping()
            logger.info("✅ Redis connected successfully")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            redis_client = None

async def send_push_notification(target_uid: str, from_uid: str):
    """Отправка push-уведомления через официальный SDK V1"""
    if not firebase_admin._apps:
        return False
    
    fcm_token = None
    if redis_client:
        fcm_token = await redis_client.get(f"fcm_token:{target_uid}")
    
    if not fcm_token:
        logger.warning(f"⚠️ No FCM token found for {target_uid}")
        return False
    
    try:
        message = messaging.Message(
            notification=messaging.Notification(
                title=f"Сообщение от {from_uid[:8]}",
                body="Зашифрованное сообщение",
            ),
            data={
                "from_uid": from_uid,
                "click_action": "FLUTTER_NOTIFICATION_CLICK"
            },
            token=fcm_token,
        )
        response = messaging.send(message)
        logger.info(f"📲 Push sent to {target_uid}: {response}")
        return True
    except Exception as e:
        logger.error(f"❌ FCM Send Error: {e}")
        return False

async def save_offline_message(uid: str, message: dict):
    if not redis_client: return
    try:
        key = f"offline:{uid}"
        await redis_client.lpush(key, json.dumps(message))
        await redis_client.expire(key, OFFLINE_MESSAGE_TTL)
        logger.info(f"💾 Saved offline message for {uid}")
    except Exception as e:
        logger.error(f"Redis save error: {e}")

async def get_offline_messages(uid: str) -> List[dict]:
    if not redis_client: return []
    try:
        key = f"offline:{uid}"
        messages_raw = await redis_client.lrange(key, 0, -1)
        await redis_client.delete(key)
        return [json.loads(m) for m in reversed(messages_raw)]
    except Exception: return []

@app.on_event("startup")
async def startup_event():
    await init_redis()
    logger.info("🚀 DeepDrift Secure Relay v3.0 started")

@app.get("/")
async def health():
    return {
        "status": "ONLINE",
        "firebase": "active" if firebase_admin._apps else "inactive",
        "redis": "connected" if redis_client else "disconnected",
        "users": len(active_connections)
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid: Optional[str] = None
    
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "init":
                my_uid = data.get("my_uid")
                active_connections[my_uid] = websocket
                logger.info(f"✅ {my_uid} connected")
                
                await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": my_uid}))
                
                # Доставка оффлайн сообщений
                offline = await get_offline_messages(my_uid)
                for msg in offline:
                    await websocket.send_text(json.dumps(msg))
                continue

            if msg_type == "register_fcm_token":
                fcm_token = data.get("fcm_token")
                if redis_client and my_uid:
                    await redis_client.set(f"fcm_token:{my_uid}", fcm_token)
                    logger.info(f"📲 FCM Token registered for {my_uid}")
                continue

            if msg_type == "message":
                target_uid = data.get("target_uid")
                payload = {
                    "type": "message",
                    "id": data.get("id"),
                    "from_uid": my_uid,
                    "encrypted_payload": data.get("encrypted_payload"),
                    "signature": data.get("signature"),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if target_uid in active_connections:
                    await active_connections[target_uid].send_text(json.dumps(payload))
                    await websocket.send_text(json.dumps({"type": "status_update", "id": data.get("id"), "status": "delivered"}))
                else:
                    await save_offline_message(target_uid, payload)
                    await send_push_notification(target_uid, my_uid)
                    await websocket.send_text(json.dumps({"type": "status_update", "id": data.get("id"), "status": "sent"}))
                continue

            if msg_type in ["typing", "read_receipt", "delivery_receipt"]:
                target = data.get("target_uid")
                if target in active_connections:
                    data["from_uid"] = my_uid
                    await active_connections[target].send_text(json.dumps(data))
                continue

    except WebSocketDisconnect:
        if my_uid in active_connections: del active_connections[my_uid]
    except Exception as e:
        logger.error(f"WS error: {e}")
    finally:
        if my_uid in active_connections: del active_connections[my_uid]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000)
