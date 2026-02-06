from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import redis.asyncio as redis
from collections import defaultdict
import secrets
import httpx
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("DDChatRelay")

app = FastAPI(title="DDChat Secure Relay", version="2.0.0")

# CORS
ALLOWED_ORIGINS = [
    "http://localhost:8080",
    "http://localhost:3000",
    "*",  # ВРЕМЕННО для тестирования
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis для персистентности
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client: Optional[redis.Redis] = None

# Firebase Cloud Messaging
FCM_SERVER_KEY = os.getenv("FCM_SERVER_KEY", "")  # ВАШ КЛЮЧ FCM
FCM_API_URL = "https://fcm.googleapis.com/fcm/send"

# In-memory хранилища
active_connections: Dict[str, WebSocket] = {}
user_tokens: Dict[str, str] = {}
message_counts = defaultdict(list)
user_public_keys: Dict[str, Dict[str, str]] = {}
user_fcm_tokens: Dict[str, str] = {}  # НОВОЕ: FCM токены пользователей

# Константы
MAX_MESSAGES_PER_MINUTE = 60
OFFLINE_MESSAGE_TTL = 604800  # 7 дней
MAX_OFFLINE_MESSAGES = 1000
PROTOCOL_VERSION = "2.0"


async def init_redis():
    """Инициализация Redis подключения"""
    global redis_client
    try:
        redis_client = await redis.from_url(
            REDIS_URL,
            encoding="utf-8",
            decode_responses=True
        )
        await redis_client.ping()
        logger.info("✅ Redis connected successfully")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        logger.warning("⚠️  Running without persistence")
        redis_client = None


async def send_push_notification(target_uid: str, from_uid: str, message_preview: str = "Новое сообщение"):
    """
    Отправляет push-уведомление через Firebase Cloud Messaging
    """
    if not FCM_SERVER_KEY:
        logger.warning("⚠️ FCM_SERVER_KEY not configured, skipping push notification")
        return False
    
    # Получаем FCM токен получателя
    fcm_token = user_fcm_tokens.get(target_uid)
    if not fcm_token and redis_client:
        try:
            fcm_token = await redis_client.get(f"fcm_token:{target_uid}")
        except Exception as e:
            logger.error(f"Failed to get FCM token from Redis: {e}")
    
    if not fcm_token:
        logger.warning(f"⚠️ No FCM token found for {target_uid}")
        return False
    
    # Формируем payload для FCM
    payload = {
        "to": fcm_token,
        "notification": {
            "title": f"Сообщение от {from_uid[:8]}...",
            "body": message_preview,
            "sound": "default",
            "badge": "1",
            "icon": "@mipmap/ic_launcher"
        },
        "data": {
            "from_uid": from_uid,
            "click_action": "FLUTTER_NOTIFICATION_CLICK"
        },
        "priority": "high"
    }
    
    headers = {
        "Authorization": f"key={FCM_SERVER_KEY}",
        "Content-Type": "application/json"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(FCM_API_URL, json=payload, headers=headers, timeout=10.0)
            
            if response.status_code == 200:
                logger.info(f"📲 Push notification sent to {target_uid}")
                return True
            else:
                logger.error(f"❌ FCM error: {response.status_code} - {response.text}")
                return False
                
    except Exception as e:
        logger.error(f"❌ Failed to send push notification: {e}")
        return False


async def save_offline_message(uid: str, message: dict):
    """Сохранение офлайн сообщения в Redis"""
    if redis_client is None:
        return
    
    try:
        key = f"offline:{uid}"
        
        count = await redis_client.llen(key)
        if count >= MAX_OFFLINE_MESSAGES:
            await redis_client.rpop(key)
        
        await redis_client.lpush(key, json.dumps(message))
        await redis_client.expire(key, OFFLINE_MESSAGE_TTL)
        
        logger.info(f"💾 Saved offline message for {uid} (total: {count + 1})")
    except Exception as e:
        logger.error(f"Failed to save offline message: {e}")


async def get_offline_messages(uid: str) -> List[dict]:
    """Получение всех офлайн сообщений из Redis"""
    if redis_client is None:
        return []
    
    try:
        key = f"offline:{uid}"
        messages_raw = await redis_client.lrange(key, 0, -1)
        await redis_client.delete(key)
        
        messages = [json.loads(m) for m in reversed(messages_raw)]
        if messages:
            logger.info(f"📬 Retrieved {len(messages)} offline messages for {uid}")
        return messages
    except Exception as e:
        logger.error(f"Failed to retrieve offline messages: {e}")
        return []


async def save_fcm_token(uid: str, fcm_token: str):
    """Сохраняет FCM токен пользователя"""
    user_fcm_tokens[uid] = fcm_token
    
    if redis_client:
        try:
            await redis_client.set(f"fcm_token:{uid}", fcm_token)
            logger.info(f"📲 Saved FCM token for {uid}")
        except Exception as e:
            logger.error(f"Failed to save FCM token: {e}")


async def save_public_key(uid: str, x25519_key: str, ed25519_key: Optional[str] = None):
    """Сохраняет публичные ключи пользователя в Redis"""
    if redis_client is None:
        user_public_keys[uid] = {
            "x25519": x25519_key,
            "ed25519": ed25519_key or ""
        }
        return
    
    try:
        key = f"pubkey:{uid}"
        data = {
            "x25519": x25519_key,
            "ed25519": ed25519_key or ""
        }
        await redis_client.set(key, json.dumps(data))
        logger.info(f"🔑 Saved public keys for {uid}")
    except Exception as e:
        logger.error(f"Failed to save public key: {e}")


async def get_public_key(uid: str) -> Optional[Dict[str, str]]:
    """Получает публичные ключи пользователя из Redis"""
    if redis_client is None:
        return user_public_keys.get(uid)
    
    try:
        key = f"pubkey:{uid}"
        data_raw = await redis_client.get(key)
        if data_raw:
            return json.loads(data_raw)
        return None
    except Exception as e:
        logger.error(f"Failed to get public key: {e}")
        return None


def generate_auth_token(uid: str) -> str:
    """Генерация токена аутентификации"""
    token = secrets.token_urlsafe(32)
    user_tokens[uid] = token
    logger.info(f"🔑 Generated auth token for {uid}")
    return token


def verify_auth_token(uid: str, token: str) -> bool:
    """Проверка токена аутентификации"""
    return user_tokens.get(uid) == token


def is_rate_limited(uid: str) -> bool:
    """Проверка rate limiting"""
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=1)
    
    message_counts[uid] = [ts for ts in message_counts[uid] if ts > cutoff]
    
    if len(message_counts[uid]) >= MAX_MESSAGES_PER_MINUTE:
        logger.warning(f"⚠️  Rate limit exceeded for {uid}")
        return True
    
    message_counts[uid].append(now)
    return False


@app.on_event("startup")
async def startup_event():
    await init_redis()
    logger.info("🚀 DDChat Secure Relay v2.0 started")
    if FCM_SERVER_KEY:
        logger.info("📲 FCM push notifications enabled")
    else:
        logger.warning("⚠️ FCM_SERVER_KEY not set - push notifications disabled")


@app.on_event("shutdown")
async def shutdown_event():
    if redis_client:
        await redis_client.close()
    logger.info("👋 Server shutting down")


@app.get("/")
async def health():
    """Health check endpoint"""
    return {
        "status": "ONLINE",
        "version": "2.0.0",
        "protocol_version": PROTOCOL_VERSION,
        "active_users": len(active_connections),
        "redis_connected": redis_client is not None,
        "fcm_enabled": bool(FCM_SERVER_KEY)
    }


@app.get("/stats")
async def stats():
    """Статистика сервера"""
    offline_counts = {}
    if redis_client:
        try:
            for uid in active_connections.keys():
                key = f"offline:{uid}"
                count = await redis_client.llen(key)
                if count > 0:
                    offline_counts[uid] = count
        except Exception as e:
            logger.error(f"Failed to get offline stats: {e}")
    
    return {
        "active_connections": len(active_connections),
        "online_users": list(active_connections.keys()),
        "offline_queues": offline_counts,
        "total_offline_messages": sum(offline_counts.values()),
        "registered_keys": len(user_public_keys),
        "registered_fcm_tokens": len(user_fcm_tokens)
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid: Optional[str] = None
    is_authenticated = False
    
    try:
        while True:
            try:
                raw = await websocket.receive_text()
                data = json.loads(raw)
                msg_type = data.get("type")
                
                logger.info(f"📥 {msg_type} from {my_uid or 'unknown'}")
                
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON"
                }))
                continue

            # === ИНИЦИАЛИЗАЦИЯ ===
            if msg_type == "init":
                my_uid = data.get("my_uid")
                provided_token = data.get("auth_token")
                protocol_version = data.get("protocol_version", "1.0")
                
                if not my_uid:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "my_uid is required"
                    }))
                    continue
                
                if protocol_version != PROTOCOL_VERSION:
                    logger.warning(f"⚠️ Protocol version mismatch: {protocol_version} != {PROTOCOL_VERSION}")
                
                # Проверка токена или создание нового
                if provided_token and verify_auth_token(my_uid, provided_token):
                    is_authenticated = True
                    logger.info(f"✅ User {my_uid} authenticated with existing token")
                else:
                    new_token = generate_auth_token(my_uid)
                    await websocket.send_text(json.dumps({
                        "type": "auth_token",
                        "token": new_token
                    }))
                    is_authenticated = True
                    logger.info(f"🔑 New token issued for {my_uid}")
                
                # Регистрируем подключение
                active_connections[my_uid] = websocket
                logger.info(f"✅ {my_uid} connected (total: {len(active_connections)})")
                
                await websocket.send_text(json.dumps({
                    "type": "uid_assigned",
                    "uid": my_uid,
                    "authenticated": is_authenticated
                }))
                
                # Отправляем офлайн сообщения
                offline_messages = await get_offline_messages(my_uid)
                for msg in offline_messages:
                    try:
                        await websocket.send_text(json.dumps(msg))
                    except Exception as e:
                        logger.error(f"Failed to send offline message: {e}")
                
                continue

            # === ПРОВЕРКА АУТЕНТИФИКАЦИИ ===
            if not is_authenticated or not my_uid:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Not authenticated"
                }))
                continue

            # === РЕГИСТРАЦИЯ FCM ТОКЕНА ===
            if msg_type == "register_fcm_token":
                fcm_token = data.get("fcm_token")
                if fcm_token:
                    await save_fcm_token(my_uid, fcm_token)
                    await websocket.send_text(json.dumps({
                        "type": "fcm_token_registered",
                        "success": True
                    }))
                    logger.info(f"📲 FCM token registered for {my_uid}")
                else:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "fcm_token is required"
                    }))
                continue

            # === РЕГИСТРАЦИЯ ПУБЛИЧНЫХ КЛЮЧЕЙ ===
            if msg_type == "register_public_key":
                x25519_key = data.get("x25519_key")
                ed25519_key = data.get("ed25519_key")
                
                if x25519_key:
                    await save_public_key(my_uid, x25519_key, ed25519_key)
                    await websocket.send_text(json.dumps({
                        "type": "public_key_registered",
                        "uid": my_uid
                    }))
                    logger.info(f"🔑 Registered public keys for {my_uid}")
                else:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "x25519_key is required"
                    }))
                continue

            # === ЗАПРОС ПУБЛИЧНОГО КЛЮЧА ===
            if msg_type == "request_public_key":
                target_uid = data.get("target_uid")
                
                if not target_uid:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "target_uid is required"
                    }))
                    continue
                
                public_keys = await get_public_key(target_uid)
                
                if public_keys:
                    await websocket.send_text(json.dumps({
                        "type": "public_key_response",
                        "target_uid": target_uid,
                        "x25519_key": public_keys.get("x25519"),
                        "ed25519_key": public_keys.get("ed25519")
                    }))
                    logger.info(f"📤 Sent public key of {target_uid} to {my_uid}")
                else:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"Public key not found for {target_uid}"
                    }))
                continue

            # === PING/PONG ===
            if msg_type == "ping":
                await websocket.send_text(json.dumps({
                    "type": "pong",
                    "timestamp": datetime.utcnow().isoformat()
                }))
                continue

            # === DELIVERY RECEIPT ===
            if msg_type == "delivery_receipt":
                target = data.get("target_uid")
                message_id = data.get("message_id")
                
                logger.info(f"📨 Delivery receipt: {my_uid} → {target} for msg {message_id}")
                
                if target in active_connections:
                    try:
                        await active_connections[target].send_text(json.dumps({
                            "type": "status_update",
                            "id": message_id,
                            "message_id": message_id,
                            "status": "delivered",
                            "from_uid": my_uid
                        }))
                        logger.info(f"✅ Sent delivery status to {target}")
                    except Exception as e:
                        logger.error(f"Failed to send delivery receipt: {e}")
                continue

            # === READ RECEIPT ===
            if msg_type == "read_receipt":
                target = data.get("target_uid")
                message_id = data.get("message_id")
                
                logger.info(f"📖 Read receipt: {my_uid} → {target} for msg {message_id}")
                
                if target in active_connections:
                    try:
                        await active_connections[target].send_text(json.dumps({
                            "type": "status_update",
                            "id": message_id,
                            "message_id": message_id,
                            "status": "read",
                            "from_uid": my_uid
                        }))
                        logger.info(f"✅ Sent read status to {target}")
                    except Exception as e:
                        logger.error(f"Failed to send read receipt: {e}")
                continue

            # === TYPING INDICATOR ===
            if msg_type == "typing":
                target = data.get("target_uid")
                is_typing = data.get("is_typing", False)
                
                if target in active_connections:
                    try:
                        await active_connections[target].send_text(json.dumps({
                            "type": "typing",
                            "from_uid": my_uid,
                            "is_typing": is_typing
                        }))
                    except Exception as e:
                        logger.error(f"Failed to send typing indicator: {e}")
                continue

            # === РОУТИНГ СООБЩЕНИЙ ===
            if msg_type == "message":
                if is_rate_limited(my_uid):
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "Rate limit exceeded"
                    }))
                    continue
                
                target_uid = data.get("target_uid")
                message_id = data.get("id")
                
                if not target_uid or not message_id:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "target_uid and id are required"
                    }))
                    continue
                
                # Формируем payload
                payload = {
                    "type": "message",
                    "id": message_id,
                    "from_uid": my_uid,
                    "encrypted_payload": data.get("encrypted_payload"),
                    "signature": data.get("signature"),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                # Доставка
                delivered = False
                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(json.dumps(payload))
                        delivered = True
                        logger.info(f"📨 Message {message_id} delivered {my_uid} → {target_uid}")
                    except Exception as e:
                        logger.error(f"Failed to deliver message: {e}")
                        delivered = False
                
                # Офлайн сохранение + PUSH
                if not delivered:
                    await save_offline_message(target_uid, payload)
                    logger.info(f"💾 Message {message_id} saved offline {my_uid} → {target_uid}")
                    
                    # ОТПРАВЛЯЕМ PUSH-УВЕДОМЛЕНИЕ
                    await send_push_notification(
                        target_uid=target_uid,
                        from_uid=my_uid,
                        message_preview="У вас новое сообщение"
                    )
                
                # ACK отправителю
                await websocket.send_text(json.dumps({
                    "type": "server_ack",
                    "id": message_id,
                    "status": "received",
                    "delivered_online": delivered,
                    "timestamp": datetime.utcnow().isoformat()
                }))
                
                logger.info(f"✅ Sent ACK to {my_uid} for msg {message_id}")
                
                # Отправляем статус "sent"
                await websocket.send_text(json.dumps({
                    "type": "status_update",
                    "id": message_id,
                    "message_id": message_id,
                    "status": "sent"
                }))
                
                continue
            
            # Неизвестный тип
            logger.warning(f"Unknown message type '{msg_type}' from {my_uid}")
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": f"Unknown message type: {msg_type}"
            }))

    except WebSocketDisconnect:
        logger.info(f"❌ UID {my_uid} disconnected")
    except Exception as e:
        logger.error(f"❌ Error for {my_uid}: {e}", exc_info=True)
    finally:
        if my_uid and active_connections.get(my_uid) == websocket:
            del active_connections[my_uid]
            logger.info(f"🔌 Cleaned up connection for {my_uid}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "server_with_push:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
