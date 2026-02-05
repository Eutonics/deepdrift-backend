from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional
import redis.asyncio as redis
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DDChatRelay")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_URL = "redis://localhost:6379" # Убедись, что в Render в Env Vars стоит верный URL
redis_client: Optional[redis.Redis] = None
active_connections: Dict[str, WebSocket] = {}
FCM_SERVER_KEY = "YOUR_FIREBASE_SERVER_KEY"

@app.on_event("startup")
async def startup_event():
    global redis_client
    try:
        # Пытаемся подключиться к Redis
        redis_client = await redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
        await redis_client.ping()
        logger.info("✅ Redis connected")
    except Exception as e:
        logger.error(f"❌ Redis error: {e}")

@app.get("/")
async def root():
    return {"status": "online", "service": "DeepDrift Relay"}

async def send_fcm_push(target_uid: str, sender_name: str):
    if not redis_client: return
    token = await redis_client.get(f"fcm_token:{target_uid}")
    if not token: return
    headers = {'Authorization': f'key={FCM_SERVER_KEY}', 'Content-Type': 'application/json'}
    payload = {
        'to': token,
        'notification': {'title': f'DeepDrift: {sender_name}', 'body': 'New message', 'sound': 'default'},
        'priority': 'high'
    }
    async with httpx.AsyncClient() as client:
        await client.post('https://fcm.googleapis.com/fcm/send', headers=headers, json=payload)

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
                logger.info(f"👤 User {my_uid} online")
                # Оффлайн сообщения (если есть Redis)
                if redis_client:
                    key = f"offline:{my_uid}"
                    msgs = await redis_client.lrange(key, 0, -1)
                    for m in reversed(msgs):
                        await websocket.send_text(m)
                    await redis_client.delete(key)
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
                    if redis_client:
                        await redis_client.lpush(f"offline:{target_uid}", json.dumps(payload))
                    await send_fcm_push(target_uid, my_uid[:8] if my_uid else "User")
                    await websocket.send_text(json.dumps({"type": "status_update", "id": data.get("id"), "status": "sent"}))

            if msg_type in ["typing", "read_receipt", "delivery_receipt"]:
                target = data.get("target_uid")
                if target in active_connections:
                    data["from_uid"] = my_uid
                    await active_connections[target].send_text(json.dumps(data))

    except WebSocketDisconnect:
        if my_uid in active_connections: del active_connections[my_uid]
    except Exception as e:
        logger.error(f"Runtime error: {e}")
    finally:
        if my_uid in active_connections: del active_connections[my_uid]
