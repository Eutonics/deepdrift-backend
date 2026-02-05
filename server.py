from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import redis.asyncio as redis
from collections import defaultdict
import hashlib
import secrets
import httpx  # Добавили для отправки уведомлений

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("DDChatRelay")

app = FastAPI(title="DDChat Secure Relay", version="3.0.0")

# CORS
ALLOWED_ORIGINS = ["*"] # Для хобби-проекта разрешаем всем, чтобы не мучаться с портами

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis для персистентности
REDIS_URL = "redis://localhost:6379"
redis_client: Optional[redis.Redis] = None

# In-memory хранилища
active_connections: Dict[str, WebSocket] = {}
user_tokens: Dict[str, str] = {}
message_counts = defaultdict(list)

# Константы
MAX_MESSAGES_PER_MINUTE = 60
OFFLINE_MESSAGE_TTL = 604800
MAX_OFFLINE_MESSAGES = 1000
PROTOCOL_VERSION = "2.0"

# --- КЛЮЧ FIREBASE (Сюда потом вставишь ключ из консоли Google) ---
FCM_SERVER_KEY = "YOUR_FIREBASE_SERVER_KEY"

async def init_redis():
    global redis_client
    try:
        redis_client = await redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        await redis_client.ping()
        logger.info("✅ Redis connected successfully")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        redis_client = None

# --- ФУНКЦИЯ ОТПРАВКИ PUSH-УВЕДОМЛЕНИЯ ---
async def send_fcm_push(target_uid: str, sender_name: str):
    if not redis_client: return
    
    # Достаем токен телефона из базы
    fcm_token = await redis_client.get(f"fcm_token:{target_uid}")
    if not fcm_token: return

    headers = {
        'Authorization': f'key={FCM_SERVER_KEY}',
        'Content-Type': 'application/json',
    }
    
    payload = {
        'to': fcm_token,
        'notification': {
            'title': f'DeepDrift: {sender_name}',
            'body': 'Новое зашифрованное сообщение',
            'sound': 'default',
        },
        'priority': 'high'
    }

    async with httpx.AsyncClient() as client:
        try:
            res = await client.post('https://fcm.googleapis.com/fcm/send', headers=headers, json=payload)
            logger.info(f"📱 Push sent to {target_uid}: {res.status_code}")
        except Exception as e:
            logger.error(f"❌ Push error: {e}")

async def save_offline_message(uid: str, message: dict):
    if redis_client is None: return
    try:
        key = f"offline:{uid}"
        await redis_client.lpush(key, json.dumps(message))
        await redis_client.expire(key, OFFLINE_MESSAGE_TTL)
    except Exception as e:
        logger.error(f"Failed to save offline: {e}")

async def get_offline_messages(uid: str) -> List[dict]:
    if redis_client is None: return []
    try:
        key = f"offline:{uid}"
        messages_raw = await redis_client.lrange(key, 0, -1)
        await redis_client.delete(key)
        return [json.loads(m) for m in reversed(messages_raw)]
    except Exception as e:
        return []

@app.on_event("startup")
async def startup_event():
    await init_redis()
    logger.info("🚀 DDChat Relay v3.0 ONLINE")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid: Optional[str] = None
    is_authenticated = False
    
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            # 1. РЕГИСТРАЦИЯ И ВХОД
            if msg_type == "init":
                my_uid = data.get("my_uid")
                active_connections[my_uid] = websocket
                is_authenticated = True
                
                # Отправляем накопившиеся сообщения
                offline = await get_offline_messages(my_uid)
                for msg in offline:
                    await websocket.send_text(json.dumps(msg))
                continue

            # 2. ПРИВЯЗКА ТЕЛЕФОНА К ПУШАМ
            if msg_type == "register_fcm_token":
                token = data.get("fcm_token")
                if redis_client and my_uid:
                    await redis_client.set(f"fcm_token:{my_uid}", token)
                    logger.info(f"📱 Token saved for {my_uid}")
                continue

            # 3. ПЕРЕСЫЛКА СООБЩЕНИЙ
            if msg_type == "message":
                target_uid = data.get("target_uid")
                message_id = data.get("id")
                
                payload = {
                    "type": "message",
                    "id": message_id,
                    "from_uid": my_uid,
                    "encrypted_payload": data.get("encrypted_payload"),
                    "signature": data.get("signature"),
                    "timestamp": datetime.utcnow().isoformat()
                }

                if target_uid in active_connections:
                    # Юзер в сети - доставляем сразу
                    await active_connections[target_uid].send_text(json.dumps(payload))
                    # Шлем отправителю статус "Доставлено" (серые галочки)
                    await websocket.send_text(json.dumps({
                        "type": "status_update", "id": message_id, "status": "delivered"
                    }))
                else:
                    # Юзер оффлайн - в базу и шлем Пуш
                    await save_offline_message(target_uid, payload)
                    await send_fcm_push(target_uid, sender_name=my_uid[:8]) # Имя - кусок UID
                    await websocket.send_text(json.dumps({
                        "type": "status_update", "id": message_id, "status": "sent"
                    }))
                continue

            # 4. СТАТУСЫ (ПЕЧАТАЕТ, ПРОЧИТАНО)
            if msg_type in ["typing", "read_receipt", "delivery_receipt"]:
                target = data.get("target_uid")
                if target in active_connections:
                    # Просто перебрасываем статус другому юзеру
                    data["from_uid"] = my_uid
                    await active_connections[target].send_text(json.dumps(data))
                continue

    except Exception as e:
        logger.error(f"WebSocket Error: {e}")
    finally:
        if my_uid in active_connections:
            del active_connections[my_uid]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000)
