from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
import redis.asyncio as redis
import firebase_admin
from firebase_admin import credentials, messaging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DDChatRelay")

app = FastAPI(title="DeepDrift Secure Relay", version="3.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Параметры из Render
REDIS_URL = os.environ.get("REDIS_URL")
FB_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

active_connections: Dict[str, WebSocket] = {}
redis_client: Optional[redis.Redis] = None

# --- ИНИЦИАЛИЗАЦИЯ FIREBASE ---
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

# --- УЛУЧШЕННОЕ ПОДКЛЮЧЕНИЕ К REDIS ---
async def init_redis():
    global redis_client
    if not REDIS_URL:
        logger.warning("⚠️ REDIS_URL not set. Offline mode disabled.")
        return

    try:
        # Хак для Render: заменяем cache:// на redis:// и отключаем проверку SSL если нужно
        url = REDIS_URL.replace("cache://", "redis://")
        
        # Создаем клиент с настройками для облачных БД
        redis_client = redis.from_url(
            url, 
            encoding="utf-8", 
            decode_responses=True,
            socket_timeout=5.0,
            retry_on_timeout=True
        )
        
        # Проверяем связь
        await redis_client.ping()
        logger.info("✅ Redis connected successfully!")
    except Exception as e:
        logger.error(f"❌ Redis Connection Failed: {e}")
        redis_client = None

@app.on_event("startup")
async def startup_event():
    await init_redis()

@app.get("/")
async def root():
    return {
        "status": "ONLINE",
        "firebase": "active" if firebase_admin._apps else "error",
        "redis": "connected" if redis_client else "disconnected",
        "users_online": len(active_connections)
    }

async def send_fcm_push(target_uid: str, from_uid: str):
    if not redis_client or not firebase_admin._apps: return
    try:
        token = await redis_client.get(f"fcm_token:{target_uid}")
        if not token: return
        
        message = messaging.Message(
            notification=messaging.Notification(
                title=f"DeepDrift: {from_uid[:8]}",
                body="Новое зашифрованное сообщение",
            ),
            token=token,
        )
        messaging.send(message)
        logger.info(f"📲 Push sent to {target_uid}")
    except Exception as e:
        logger.error(f"❌ Push Send Error: {e}")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid = None
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "init":
                my_uid = data.get("my_uid")
                active_connections[my_uid] = websocket
                logger.info(f"👤 {my_uid} connected")
                await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": my_uid}))
                
                if redis_client:
                    key = f"offline:{my_uid}"
                    msgs = await redis_client.lrange(key, 0, -1)
                    for m in reversed(msgs): await websocket.send_text(m)
                    await redis_client.delete(key)
                continue

            if msg_type == "register_fcm_token":
                token = data.get("fcm_token")
                if redis_client and my_uid:
                    await redis_client.set(f"fcm_token:{my_uid}", token)
                    logger.info(f"📱 Token registered for {my_uid}")
                continue

            if msg_type == "message":
                target_uid = data.get("target_uid")
                msg_id = data.get("id")
                payload = {
                    "type": "message", "id": msg_id, "from_uid": my_uid,
                    "encrypted_payload": data.get("encrypted_payload"),
                    "signature": data.get("signature"),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                if target_uid in active_connections:
                    await active_connections[target_uid].send_text(json.dumps(payload))
                    await websocket.send_text(json.dumps({"type": "status_update", "id": msg_id, "status": "delivered"}))
                else:
                    if redis_client:
                        await redis_client.lpush(f"offline:{target_uid}", json.dumps(payload))
                    await send_fcm_push(target_uid, my_uid or "User")
                    await websocket.send_text(json.dumps({"type": "status_update", "id": msg_id, "status": "sent"}))
                continue

            if msg_type in ["typing", "read_receipt", "delivery_receipt"]:
                target = data.get("target_uid")
                if target in active_connections:
                    data["from_uid"] = my_uid
                    await active_connections[target].send_text(json.dumps(data))

    except Exception: pass
    finally:
        if my_uid in active_connections: del active_connections[my_uid]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000)
