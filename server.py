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
    # В продакшене добавьте свой домен
]

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
user_public_keys: Dict[str, Dict[str, str]] = {}  # uid -> {x25519_key, ed25519_key}

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


async def save_public_key(uid: str, x25519_key: str, ed25519_key: Optional[str] = None):
    """Сохраняет публичные ключи пользователя в Redis"""
    if redis_client is None:
        # Fallback на in-memory
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
        # Публичные ключи не истекают
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
        "redis_connected": redis_client is not None
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
        "registered_keys": len(user_public_keys)
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
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON from {my_uid or 'unknown'}: {e}")
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Invalid JSON format"
                }))
                continue
            
            msg_type = data.get("type")

            # === ИНИЦИАЛИЗАЦИЯ ===
            if msg_type == "init":
                protocol_version = data.get("protocol_version", "1.0")
                
                if protocol_version != PROTOCOL_VERSION:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"Unsupported protocol version. Expected {PROTOCOL_VERSION}"
                    }))
                    await websocket.close(code=1003)
                    return
                
                my_uid = data.get("my_uid")
                provided_token = data.get("auth_token")
                
                if not my_uid:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "UID is required"
                    }))
                    continue
                
                # Аутентификация
                if provided_token and verify_auth_token(my_uid, provided_token):
                    is_authenticated = True
                else:
                    new_token = generate_auth_token(my_uid)
                    is_authenticated = True
                    await websocket.send_text(json.dumps({
                        "type": "auth_token",
                        "token": new_token,
                        "message": "Save this token for future connections"
                    }))
                
                active_connections[my_uid] = websocket
                logger.info(f"✅ UID {my_uid} connected")
                
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
                
                if target in active_connections:
                    try:
                        await active_connections[target].send_text(json.dumps({
                            "type": "status_update",
                            "id": message_id,
                            "status": "delivered",
                            "from_uid": my_uid
                        }))
                    except Exception as e:
                        logger.error(f"Failed to send delivery receipt: {e}")
                continue

            # === READ RECEIPT ===
            if msg_type == "read_receipt":
                target = data.get("target_uid")
                message_id = data.get("message_id")
                
                if target in active_connections:
                    try:
                        await active_connections[target].send_text(json.dumps({
                            "type": "status_update",
                            "id": message_id,
                            "status": "read",
                            "from_uid": my_uid
                        }))
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
                    "signature": data.get("signature"),  # Ed25519 подпись
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                # Доставка
                delivered = False
                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(json.dumps(payload))
                        delivered = True
                        logger.info(f"📨 Message {message_id} delivered {my_uid} -> {target_uid}")
                    except Exception as e:
                        logger.error(f"Failed to deliver message: {e}")
                        delivered = False
                
                # Офлайн сохранение
                if not delivered:
                    await save_offline_message(target_uid, payload)
                    logger.info(f"💾 Message {message_id} saved offline {my_uid} -> {target_uid}")
                
                # ACK отправителю
                await websocket.send_text(json.dumps({
                    "type": "server_ack",
                    "id": message_id,
                    "status": "received",
                    "delivered_online": delivered,
                    "timestamp": datetime.utcnow().isoformat()
                }))
                
                await websocket.send_text(json.dumps({
                    "type": "status_update",
                    "id": message_id,
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
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
