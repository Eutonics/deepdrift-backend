from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import os
from datetime import datetime
from typing import Dict, Optional
import redis.asyncio as redis
import httpx

# Настройка логирования
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

# Берем URL из Environment Variables Render
REDIS_URL = os.environ.get("REDIS_URL")
FCM_SERVER_KEY = os.environ.get("FCM_SERVER_KEY", "YOUR_FCM_KEY")

redis_client: Optional[redis.Redis] = None
active_connections: Dict[str, WebSocket] = {}

# ✅ In-memory storage для работы без Redis (для тестирования)
in_memory_storage = {
    "pubkeys": {},      # {uid: {"x25519": "...", "ed25519": "..."}}
    "offline": {},      # {uid: [messages]}
    "fcm_tokens": {}    # {uid: "token"}
}

@app.on_event("startup")
async def startup_event():
    global redis_client
    if REDIS_URL:
        try:
            # Render иногда дает префикс rediss:// для защищенного соединения
            redis_client = await redis.from_url(
                REDIS_URL, 
                encoding="utf-8", 
                decode_responses=True,
                ssl_cert_reqs=None
            )
            await redis_client.ping()
            logger.info("✅ Redis connected successfully")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            logger.warning("⚠️ Falling back to in-memory storage")
            redis_client = None
    else:
        logger.warning("⚠️ REDIS_URL not found, using in-memory storage")

@app.get("/")
async def root():
    return {
        "status": "online",
        "redis_active": redis_client is not None,
        "active_users": len(active_connections),
        "storage_mode": "redis" if redis_client else "in-memory"
    }

async def send_fcm_push(target_uid: str, sender_name: str):
    if not FCM_SERVER_KEY: return
    try:
        # Попытка получить токен из Redis или памяти
        token = None
        if redis_client:
            token = await redis_client.get(f"fcm_token:{target_uid}")
        else:
            token = in_memory_storage["fcm_tokens"].get(target_uid)
            
        if not token: return
        
        headers = {
            'Authorization': f'key={FCM_SERVER_KEY}',
            'Content-Type': 'application/json',
        }
        payload = {
            'to': token,
            'notification': {
                'title': f'DeepDrift: {sender_name}',
                'body': 'New encrypted message',
                'sound': 'default',
            },
            'priority': 'high'
        }
        async with httpx.AsyncClient() as client:
            await client.post('https://fcm.googleapis.com/fcm/send', headers=headers, json=payload)
    except Exception as e:
        logger.error(f"Push error: {e}")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid = None
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")
            
            logger.info(f"📨 Received: {msg_type} from {my_uid or 'unknown'}")

            if msg_type == "init":
                my_uid = data.get("my_uid")
                protocol_version = data.get("protocol_version", "1.0")
                
                # Validate protocol version
                if protocol_version != "2.0":
                    logger.warning(f"⚠️ Unsupported protocol version: {protocol_version}")
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"Unsupported protocol version: {protocol_version}. Server requires 2.0"
                    }))
                    await websocket.close()
                    return
                
                active_connections[my_uid] = websocket
                logger.info(f"👤 User {my_uid} connected (protocol v{protocol_version})")
                
                # Send uid_assigned confirmation
                await websocket.send_text(json.dumps({
                    "type": "uid_assigned",
                    "uid": my_uid,
                    "server_version": "1.0"
                }))
                
                # Retrieve offline messages
                try:
                    if redis_client:
                        key = f"offline:{my_uid}"
                        msgs = await redis_client.lrange(key, 0, -1)
                        for m in reversed(msgs):
                            await websocket.send_text(m)
                        await redis_client.delete(key)
                    else:
                        # Use in-memory storage
                        if my_uid in in_memory_storage["offline"]:
                            for m in reversed(in_memory_storage["offline"][my_uid]):
                                await websocket.send_text(m)
                            in_memory_storage["offline"][my_uid] = []
                except Exception as e:
                    logger.error(f"Offline retrieval error: {e}")
                continue

            if msg_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue

            if msg_type == "register_public_key":
                x25519_key = data.get("x25519_key")
                ed25519_key = data.get("ed25519_key")
                
                if not my_uid:
                    logger.error("❌ No UID set, cannot register keys")
                    continue
                
                try:
                    if redis_client:
                        # Use Redis
                        await redis_client.set(f"pubkey_x25519:{my_uid}", x25519_key)
                        await redis_client.set(f"pubkey_ed25519:{my_uid}", ed25519_key)
                    else:
                        # Use in-memory storage
                        in_memory_storage["pubkeys"][my_uid] = {
                            "x25519": x25519_key,
                            "ed25519": ed25519_key
                        }
                    
                    await websocket.send_text(json.dumps({
                        "type": "public_key_registered",
                        "success": True
                    }))
                    logger.info(f"🔑 Keys registered for {my_uid}")
                except Exception as e:
                    logger.error(f"Failed to register keys: {e}")
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"Failed to register public keys: {str(e)}"
                    }))
                continue

            if msg_type == "request_public_key":
                target_uid = data.get("target_uid")
                
                try:
                    x25519_key = None
                    ed25519_key = None
                    
                    if redis_client:
                        # Use Redis
                        x25519_key = await redis_client.get(f"pubkey_x25519:{target_uid}")
                        ed25519_key = await redis_client.get(f"pubkey_ed25519:{target_uid}")
                    else:
                        # Use in-memory storage
                        if target_uid in in_memory_storage["pubkeys"]:
                            x25519_key = in_memory_storage["pubkeys"][target_uid]["x25519"]
                            ed25519_key = in_memory_storage["pubkeys"][target_uid]["ed25519"]
                    
                    await websocket.send_text(json.dumps({
                        "type": "public_key_response",
                        "target_uid": target_uid,
                        "x25519_key": x25519_key,
                        "ed25519_key": ed25519_key
                    }))
                    logger.info(f"🔑 Sent keys for {target_uid} to {my_uid}")
                except Exception as e:
                    logger.error(f"Failed to retrieve keys: {e}")
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"Failed to retrieve keys for {target_uid}: {str(e)}"
                    }))
                continue

            if msg_type == "message":
                target_uid = data.get("target_uid")
                msg_id = data.get("id")
                payload = {
                    "type": "message",
                    "id": msg_id,
                    "from_uid": my_uid,
                    "encrypted_payload": data.get("encrypted_payload"),
                    "signature": data.get("signature"),
                    "timestamp": datetime.utcnow().isoformat()
                }

                if target_uid in active_connections:
                    await active_connections[target_uid].send_text(json.dumps(payload))
                    await websocket.send_text(json.dumps({
                        "type": "status_update", "id": msg_id, "status": "delivered"
                    }))
                else:
                    # Save offline message
                    try:
                        if redis_client:
                            await redis_client.lpush(f"offline:{target_uid}", json.dumps(payload))
                        else:
                            if target_uid not in in_memory_storage["offline"]:
                                in_memory_storage["offline"][target_uid] = []
                            in_memory_storage["offline"][target_uid].append(json.dumps(payload))
                    except Exception as e:
                        logger.error(f"Offline save error: {e}")
                        
                    await send_fcm_push(target_uid, my_uid[:8] if my_uid else "User")
                    await websocket.send_text(json.dumps({
                        "type": "status_update", "id": msg_id, "status": "sent"
                    }))

            if msg_type in ["typing", "read_receipt", "delivery_receipt"]:
                target = data.get("target_uid")
                if target in active_connections:
                    data["from_uid"] = my_uid
                    await active_connections[target].send_text(json.dumps(data))

            if msg_type == "register_fcm_token":
                token = data.get("fcm_token")
                if my_uid:
                    try:
                        if redis_client:
                            await redis_client.set(f"fcm_token:{my_uid}", token)
                        else:
                            in_memory_storage["fcm_tokens"][my_uid] = token
                    except Exception as e:
                        logger.error(f"Failed to save FCM token: {e}")

    except WebSocketDisconnect:
        if my_uid and my_uid in active_connections:
            del active_connections[my_uid]
        logger.info(f"👋 User {my_uid} disconnected")
    except Exception as e:
        logger.error(f"❌ WS Runtime error: {e}", exc_info=True)  # ✅ Added exc_info for full stack trace
        if my_uid and my_uid in active_connections:
            del active_connections[my_uid]
    finally:
        if my_uid and my_uid in active_connections:
            del active_connections[my_uid]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000)
