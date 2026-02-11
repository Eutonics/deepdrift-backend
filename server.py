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

app = FastAPI(title="DeepDrift Secure Relay Enhanced", version="4.0.0")
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

# --- ПОДКЛЮЧЕНИЕ К REDIS ---
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
        "version": "4.0.0",
        "firebase": "active" if firebase_admin._apps else "error",
        "redis": "connected" if redis_client else "disconnected",
        "users_online": len(active_connections),
        "features": [
            "delete_message",
            "edit_message",
            "message_reaction",
            "forward_message",
            "read_receipt",
            "delivery_receipt",
            "voice_messages",
            "photo_messages"
        ]
    }

async def send_fcm_push(target_uid: str, from_uid: str, message_type: str = "new_message"):
    if not redis_client or not firebase_admin._apps: return
    try:
        token = await redis_client.get(f"fcm_token:{target_uid}")
        if not token: return
        
        title_map = {
            "new_message": f"DeepDrift: {from_uid[:8]}",
            "message_deleted": "Message deleted",
            "message_edited": "Message edited",
            "message_reaction": "New reaction"
        }
        
        body_map = {
            "new_message": "Новое зашифрованное сообщение",
            "message_deleted": "Сообщение было удалено",
            "message_edited": "Сообщение было изменено",
            "message_reaction": "Новая реакция на сообщение"
        }
        
        message = messaging.Message(
            notification=messaging.Notification(
                title=title_map.get(message_type, "DeepDrift"),
                body=body_map.get(message_type, "Новое событие"),
            ),
            data={
                "from_uid": from_uid,
                "type": message_type,
            },
            token=token,
        )
        messaging.send(message)
        logger.info(f"📲 Push sent to {target_uid} ({message_type})")
    except Exception as e:
        logger.error(f"❌ Push Send Error: {e}")

async def send_offline_messages(websocket: WebSocket, my_uid: str):
    """Отправка оффлайн сообщений при подключении"""
    if not redis_client: return
    
    import asyncio
    await asyncio.sleep(0.5)  # Даём время клиенту инициализироваться
    
    try:
        offline_key = f"offline_queue:{my_uid}"
        messages = await redis_client.lrange(offline_key, 0, -1)
        
        if messages:
            logger.info(f"📬 Sending {len(messages)} offline messages to {my_uid}")
            
            for msg_json in messages:
                try:
                    msg_data = json.loads(msg_json)
                    await websocket.send_text(msg_json)
                    logger.info(f"📨 Sent offline message to {my_uid}")
                except Exception as e:
                    logger.error(f"❌ Failed to send offline message: {e}")
            
            await redis_client.delete(offline_key)
            logger.info(f"🗑️ Cleared offline queue for {my_uid}")
    except Exception as e:
        logger.error(f"❌ Error sending offline messages: {e}")

async def store_offline_message(target_uid: str, message_data: dict):
    """Сохранение сообщения для оффлайн доставки"""
    if not redis_client: return
    
    try:
        offline_key = f"offline_queue:{target_uid}"
        await redis_client.rpush(offline_key, json.dumps(message_data))
        await redis_client.expire(offline_key, 7 * 24 * 3600)  # 7 дней
        logger.info(f"💾 Stored offline message for {target_uid}")
    except Exception as e:
        logger.error(f"❌ Failed to store offline message: {e}")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid: Optional[str] = None
    
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            # === ИНИЦИАЛИЗАЦИЯ ===
            if msg_type == "init":
                my_uid = data.get("my_uid")
                
                if not my_uid:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "my_uid is required"
                    }))
                    continue

                active_connections[my_uid] = websocket
                logger.info(f"✅ {my_uid} connected (total: {len(active_connections)})")
                
                await websocket.send_text(json.dumps({
                    "type": "uid_assigned",
                    "my_uid": my_uid,
                }))
                
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
                        else:
                            await websocket.send_text(json.dumps({
                                "type": "public_key_response",
                                "target_uid": target_uid,
                                "error": "keys_not_found"
                            }))
                    except Exception as e:
                        logger.error(f"❌ Error retrieving keys: {e}")
                continue

            # === ОТПРАВКА СООБЩЕНИЯ ===
            if msg_type == "message":
                target_uid = data.get("target_uid")
                encrypted_text = data.get("encrypted_text")
                signature = data.get("signature")
                message_id = data.get("id")
                reply_to_id = data.get("replyToId")
                message_type = data.get("messageType", "text")
                media_data = data.get("mediaData")
                
                if not all([target_uid, encrypted_text, message_id]):
                    continue

                message_payload = {
                    "type": "message",
                    "from_uid": my_uid,
                    "id": message_id,
                    "encrypted_text": encrypted_text,
                    "signature": signature,
                    "time": int(datetime.now().timestamp() * 1000),
                    "replyToId": reply_to_id,
                    "messageType": message_type,
                    "mediaData": media_data,
                }

                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(
                            json.dumps(message_payload)
                        )
                        logger.info(f"📨 Message delivered online: {my_uid} -> {target_uid}")
                    except Exception as e:
                        logger.error(f"❌ Failed to deliver: {e}")
                        await store_offline_message(target_uid, message_payload)
                else:
                    await store_offline_message(target_uid, message_payload)
                    await send_fcm_push(target_uid, my_uid, "new_message")

                continue

            # === УДАЛЕНИЕ СООБЩЕНИЯ ===
            if msg_type == "delete_message":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                
                if not all([target_uid, message_id]):
                    continue

                delete_payload = {
                    "type": "message_deleted",
                    "from_uid": my_uid,
                    "message_id": message_id,
                    "time": int(datetime.now().timestamp() * 1000),
                }

                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(
                            json.dumps(delete_payload)
                        )
                        logger.info(f"🗑️ Delete request delivered: {message_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to deliver delete: {e}")
                        await store_offline_message(target_uid, delete_payload)
                else:
                    await store_offline_message(target_uid, delete_payload)
                    await send_fcm_push(target_uid, my_uid, "message_deleted")

                continue

            # === РЕДАКТИРОВАНИЕ СООБЩЕНИЯ ===
            if msg_type == "edit_message":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                new_encrypted_text = data.get("new_encrypted_text")
                new_signature = data.get("new_signature")
                
                if not all([target_uid, message_id, new_encrypted_text]):
                    continue

                edit_payload = {
                    "type": "message_edited",
                    "from_uid": my_uid,
                    "message_id": message_id,
                    "new_encrypted_text": new_encrypted_text,
                    "new_signature": new_signature,
                    "time": int(datetime.now().timestamp() * 1000),
                }

                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(
                            json.dumps(edit_payload)
                        )
                        logger.info(f"✏️ Edit delivered: {message_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to deliver edit: {e}")
                        await store_offline_message(target_uid, edit_payload)
                else:
                    await store_offline_message(target_uid, edit_payload)
                    await send_fcm_push(target_uid, my_uid, "message_edited")

                continue

            # === РЕАКЦИЯ НА СООБЩЕНИЕ ===
            if msg_type == "message_reaction":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                emoji = data.get("emoji")
                action = data.get("action")  # 'add' or 'remove'
                
                if not all([target_uid, message_id, emoji, action]):
                    continue

                reaction_payload = {
                    "type": "message_reaction",
                    "from_uid": my_uid,
                    "message_id": message_id,
                    "emoji": emoji,
                    "action": action,
                    "time": int(datetime.now().timestamp() * 1000),
                }

                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(
                            json.dumps(reaction_payload)
                        )
                        logger.info(f"{emoji} Reaction delivered: {emoji} on {message_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to deliver reaction: {e}")
                else:
                    await send_fcm_push(target_uid, my_uid, "message_reaction")

                continue

            # === ПЕРЕСЫЛКА СООБЩЕНИЯ ===
            if msg_type == "forward_message":
                target_uid = data.get("target_uid")
                original_message_id = data.get("original_message_id")
                forwarded_from = data.get("forwarded_from")
                encrypted_text = data.get("encrypted_text")
                signature = data.get("signature")
                new_message_id = data.get("id")
                
                if not all([target_uid, original_message_id, encrypted_text, new_message_id]):
                    continue

                forward_payload = {
                    "type": "message",
                    "from_uid": my_uid,
                    "id": new_message_id,
                    "encrypted_text": encrypted_text,
                    "signature": signature,
                    "time": int(datetime.now().timestamp() * 1000),
                    "forwarded_from": forwarded_from,
                    "original_message_id": original_message_id,
                }

                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(
                            json.dumps(forward_payload)
                        )
                        logger.info(f"↪️ Forward delivered: {new_message_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to forward: {e}")
                        await store_offline_message(target_uid, forward_payload)
                else:
                    await store_offline_message(target_uid, forward_payload)
                    await send_fcm_push(target_uid, my_uid, "new_message")

                continue

            # === READ RECEIPT ===
            if msg_type == "read_receipt":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                
                if not all([target_uid, message_id]):
                    continue

                read_payload = {
                    "type": "read_receipt",
                    "from_uid": my_uid,
                    "message_id": message_id,
                    "time": int(datetime.now().timestamp() * 1000),
                }

                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(
                            json.dumps(read_payload)
                        )
                        logger.info(f"✓✓ Read receipt delivered: {message_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to send read receipt: {e}")

                continue

            # === DELIVERY RECEIPT ===
            if msg_type == "delivery_receipt":
                target_uid = data.get("target_uid")
                message_id = data.get("message_id")
                
                if not all([target_uid, message_id]):
                    continue

                delivery_payload = {
                    "type": "delivery_receipt",
                    "from_uid": my_uid,
                    "message_id": message_id,
                    "time": int(datetime.now().timestamp() * 1000),
                }

                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(
                            json.dumps(delivery_payload)
                        )
                        logger.info(f"✓ Delivery receipt sent: {message_id}")
                    except Exception as e:
                        logger.error(f"❌ Failed to send delivery receipt: {e}")

                continue

            # === TYPING INDICATOR ===
            if msg_type == "typing_indicator":
                target_uid = data.get("target_uid")
                typing = data.get("typing", False)
                
                if target_uid and target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(
                            json.dumps({
                                "type": "typing_indicator",
                                "from_uid": my_uid,
                                "typing": typing
                            })
                        )
                    except Exception as e:
                        logger.error(f"❌ Failed to send typing indicator: {e}")

                continue

            # === PING/PONG ===
            if msg_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue

    except WebSocketDisconnect:
        if my_uid:
            active_connections.pop(my_uid, None)
            logger.info(f"👋 {my_uid} disconnected (total: {len(active_connections)})")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
        if my_uid:
            active_connections.pop(my_uid, None)
