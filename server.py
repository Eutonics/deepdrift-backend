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

app = FastAPI(title="DeepDrift Secure Relay", version="3.3.1")
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
        url = REDIS_URL.replace("cache://", "redis://")
        
        redis_client = redis.from_url(
            url, 
            encoding="utf-8", 
            decode_responses=True,
            socket_timeout=5.0,
            retry_on_timeout=True
        )
        
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
        "version": "3.3.1",
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
            data={
                "from_uid": from_uid,
                "type": "new_message",
            },
            token=token,
        )
        messaging.send(message)
        logger.info(f"📲 Push sent to {target_uid}")
    except Exception as e:
        logger.error(f"❌ Push Send Error: {e}")

# НОВОЕ: Асинхронная отправка оффлайн сообщений с задержкой
async def send_offline_messages(websocket: WebSocket, my_uid: str):
    """
    Отправляет оффлайн сообщения с небольшой задержкой,
    чтобы дать клиенту время зарегистрировать ключи
    """
    import asyncio
    
    # Ждём 500ms чтобы клиент успел зарегистрировать FCM токен и публичные ключи
    await asyncio.sleep(0.5)
    
    if not redis_client:
        return
    
    try:
        key = f"offline:{my_uid}"
        msgs = await redis_client.lrange(key, 0, -1)
        
        if msgs:
            logger.info(f"📬 Sending {len(msgs)} offline messages to {my_uid}")
            
            for m in reversed(msgs):
                try:
                    await websocket.send_text(m)
                    logger.info(f"📤 Sent offline message to {my_uid}")
                except Exception as e:
                    logger.error(f"❌ Failed to send offline message: {e}")
                    return  # Останавливаемся если соединение оборвалось
            
            # Очищаем очередь только если все сообщения отправлены успешно
            await redis_client.delete(key)
            logger.info(f"✅ Cleared offline queue for {my_uid}")
    except Exception as e:
        logger.error(f"❌ Error sending offline messages: {e}")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid = None
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            # === ИНИЦИАЛИЗАЦИЯ ПОДКЛЮЧЕНИЯ ===
            if msg_type == "init":
                my_uid = data.get("my_uid")
                active_connections[my_uid] = websocket
                logger.info(f"👤 {my_uid} connected")
                await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": my_uid}))
                
                # ИСПРАВЛЕНИЕ: Отправляем оффлайн сообщения асинхронно с задержкой
                # Это даёт клиенту время зарегистрировать FCM токен и ключи
                import asyncio
                asyncio.create_task(send_offline_messages(websocket, my_uid))
                
                continue

            # === РЕГИСТРАЦИЯ FCM ТОКЕНА ===
            if msg_type == "register_fcm_token":
                token = data.get("fcm_token")
                if redis_client and my_uid:
                    await redis_client.set(f"fcm_token:{my_uid}", token)
                    logger.info(f"📱 Token registered for {my_uid}")
                    await websocket.send_text(json.dumps({
                        "type": "fcm_token_registered",
                        "status": "success"
                    }))
                continue

            # === РЕГИСТРАЦИЯ ПУБЛИЧНЫХ КЛЮЧЕЙ ===
            if msg_type == "register_public_key":
                x25519_key = data.get("x25519_key")
                ed25519_key = data.get("ed25519_key")
                
                if redis_client and my_uid and x25519_key and ed25519_key:
                    await redis_client.setex(
                        f"pubkey:{my_uid}:x25519",
                        30 * 24 * 3600,
                        x25519_key
                    )
                    await redis_client.setex(
                        f"pubkey:{my_uid}:ed25519",
                        30 * 24 * 3600,
                        ed25519_key
                    )
                    logger.info(f"🔑 Public keys registered for {my_uid}")
                    
                    await websocket.send_text(json.dumps({
                        "type": "public_key_registered",
                        "status": "success"
                    }))
                else:
                    logger.warning(f"⚠️ Failed to register keys for {my_uid}")
                continue

            # === ЗАПРОС ПУБЛИЧНЫХ КЛЮЧЕЙ ===
            if msg_type == "request_public_key":
                target_uid = data.get("target_uid")
                
                if redis_client and target_uid:
                    try:
                        x25519_key = await redis_client.get(f"pubkey:{target_uid}:x25519")
                        ed25519_key = await redis_client.get(f"pubkey:{target_uid}:ed25519")
                        
                        if x25519_key and ed25519_key:
                            response = {
                                "type": "public_key_response",
                                "target_uid": target_uid,
                                "x25519_key": x25519_key,
                                "ed25519_key": ed25519_key
                            }
                            await websocket.send_text(json.dumps(response))
                            logger.info(f"🔑 Sent public keys of {target_uid} to {my_uid}")
                            
                            # Автоматический взаимный обмен ключами
                            if target_uid in active_connections and my_uid:
                                my_x25519 = await redis_client.get(f"pubkey:{my_uid}:x25519")
                                my_ed25519 = await redis_client.get(f"pubkey:{my_uid}:ed25519")
                                
                                if my_x25519 and my_ed25519:
                                    reverse_response = {
                                        "type": "public_key_response",
                                        "target_uid": my_uid,
                                        "x25519_key": my_x25519,
                                        "ed25519_key": my_ed25519
                                    }
                                    try:
                                        await active_connections[target_uid].send_text(
                                            json.dumps(reverse_response)
                                        )
                                        logger.info(f"🔄 Auto-sent public keys of {my_uid} to {target_uid}")
                                    except Exception as e:
                                        logger.error(f"❌ Failed to auto-send keys to {target_uid}: {e}")
                        else:
                            await websocket.send_text(json.dumps({
                                "type": "public_key_response",
                                "target_uid": target_uid,
                                "error": "keys_not_found"
                            }))
                            logger.warning(f"⚠️ Public keys not found for {target_uid}")
                    except Exception as e:
                        logger.error(f"❌ Error retrieving keys for {target_uid}: {e}")
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "message": f"Failed to retrieve keys: {str(e)}"
                        }))
                continue

            # === ОТПРАВКА СООБЩЕНИЯ ===
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
                
                # Проверяем, онлайн ли получатель
                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(json.dumps(payload))
                        await websocket.send_text(json.dumps({
                            "type": "status_update", 
                            "id": msg_id, 
                            "status": "delivered"
                        }))
                        logger.info(f"📨 Message {msg_id} delivered to {target_uid}")
                    except Exception as e:
                        logger.error(f"❌ Failed to deliver to {target_uid}: {e}")
                        if redis_client:
                            await redis_client.lpush(f"offline:{target_uid}", json.dumps(payload))
                        await websocket.send_text(json.dumps({
                            "type": "status_update", 
                            "id": msg_id, 
                            "status": "sent"
                        }))
                else:
                    # Получатель оффлайн - сохраняем в Redis
                    if redis_client:
                        await redis_client.lpush(f"offline:{target_uid}", json.dumps(payload))
                        logger.info(f"💾 Message {msg_id} saved for offline {target_uid}")
                    
                    # Отправляем push-уведомление
                    await send_fcm_push(target_uid, my_uid or "User")
                    
                    await websocket.send_text(json.dumps({
                        "type": "status_update", 
                        "id": msg_id, 
                        "status": "sent"
                    }))
                continue

            # === PING/PONG для heartbeat ===
            if msg_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue

            # === ИНДИКАТОРЫ НАБОРА ТЕКСТА И ПРОЧТЕНИЯ ===
            if msg_type in ["typing", "read_receipt", "delivery_receipt"]:
                target = data.get("target_uid")
                if target in active_connections:
                    data["from_uid"] = my_uid
                    try:
                        await active_connections[target].send_text(json.dumps(data))
                        logger.debug(f"📤 {msg_type} sent to {target}")
                    except Exception as e:
                        logger.error(f"❌ Failed to send {msg_type} to {target}: {e}")
                continue

            # Неизвестный тип сообщения
            logger.warning(f"⚠️ Unknown message type: {msg_type} from {my_uid}")

    except WebSocketDisconnect:
        logger.info(f"👋 {my_uid} disconnected normally")
    except Exception as e:
        logger.error(f"❌ WebSocket error for {my_uid}: {e}")
    finally:
        if my_uid in active_connections: 
            del active_connections[my_uid]
            logger.info(f"🧹 Cleaned up connection for {my_uid}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000)
