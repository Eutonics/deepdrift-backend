from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional
import redis.asyncio as redis
import httpx

# Логирование
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DDChatRelay")

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# --- ЧИТАЕМ REDIS_URL ИЗ ПАНЕЛИ RENDER ---
# Если переменная не найдена, сервер не упадет, а просто выключит оффлайн-режим
REDIS_URL = os.environ.get("REDIS_URL")
redis_client: Optional[redis.Redis] = None
active_connections: Dict[str, WebSocket] = {}

@app.on_event("startup")
async def startup_event():
    global redis_client
    if REDIS_URL:
        try:
            # Убираем возможные проблемы с протоколом redis/rediss
            url = REDIS_URL.replace("cache://", "redis://")
            redis_client = await redis.from_url(url, encoding="utf-8", decode_responses=True)
            await redis_client.ping()
            logger.info("✅ Connected to Render Redis")
        except Exception as e:
            logger.error(f"❌ Redis Connection Error: {e}")
            redis_client = None
    else:
        logger.warning("⚠️ REDIS_URL not found in environment variables")

@app.get("/")
async def root():
    return {
        "status": "online",
        "redis_active": redis_client is not None,
        "connections": len(active_connections)
    }

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
                logger.info(f"👤 User {my_uid} connected")
                
                if redis_client:
                    try:
                        key = f"offline:{my_uid}"
                        msgs = await redis_client.lrange(key, 0, -1)
                        for m in reversed(msgs): await websocket.send_text(m)
                        await redis_client.delete(key)
                    except: pass
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
                        try: await redis_client.lpush(f"offline:{target_uid}", json.dumps(payload))
                        except: pass
                    # Push уведомление (если настроен ключ)
                    await websocket.send_text(json.dumps({"type": "status_update", "id": data.get("id"), "status": "sent"}))

            if msg_type in ["typing", "read_receipt", "delivery_receipt"]:
                target = data.get("target_uid")
                if target in active_connections:
                    data["from_uid"] = my_uid
                    await active_connections[target].send_text(json.dumps(data))

    except Exception:
        pass
    finally:
        if my_uid in active_connections:
            del active_connections[my_uid]
