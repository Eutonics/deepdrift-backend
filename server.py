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
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI(title="DeepDrift Secure Relay", version="2.0.0")

# CORS - ВАЖНО: замените на ваш домен в продакшене!
ALLOWED_ORIGINS = [
    "http://localhost:8080",
    "http://localhost:3000",
    # В продакшене добавьте:
    # "https://yourdomain.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis для персистентности офлайн сообщений
REDIS_URL = "redis://localhost:6379"  # Или используйте env переменную
redis_client: Optional[redis.Redis] = None

# In-memory хранилища
active_connections: Dict[str, WebSocket] = {}
user_tokens: Dict[str, str] = {}  # uid -> token для аутентификации
message_counts = defaultdict(list)  # uid -> [timestamps] для rate limiting

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
        logger.warning("⚠️  Running without persistence - offline messages will be lost on restart")
        redis_client = None


async def save_offline_message(uid: str, message: dict):
    """Сохранение офлайн сообщения в Redis"""
    if redis_client is None:
        # Fallback на in-memory (не рекомендуется для продакшена)
        return
    
    try:
        key = f"offline:{uid}"
        
        # Проверяем количество сообщений
        count = await redis_client.llen(key)
        if count >= MAX_OFFLINE_MESSAGES:
            # Удаляем самое старое сообщение
            await redis_client.rpop(key)
        
        # Добавляем новое сообщение
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


def generate_auth_token(uid: str) -> str:
    """Генерация токена аутентификации для пользователя"""
    # В реальном приложении используйте JWT
    token = secrets.token_urlsafe(32)
    user_tokens[uid] = token
    logger.info(f"🔑 Generated auth token for {uid}")
    return token


def verify_auth_token(uid: str, token: str) -> bool:
    """Проверка токена аутентификации"""
    return user_tokens.get(uid) == token


def is_rate_limited(uid: str) -> bool:
    """Проверка rate limiting для пользователя"""
    now = datetime.utcnow()
    cutoff = now - timedelta(minutes=1)
    
    # Очищаем старые записи
    message_counts[uid] = [ts for ts in message_counts[uid] if ts > cutoff]
    
    if len(message_counts[uid]) >= MAX_MESSAGES_PER_MINUTE:
        logger.warning(f"⚠️  Rate limit exceeded for {uid}")
        return True
    
    message_counts[uid].append(now)
    return False


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске приложения"""
    await init_redis()
    logger.info("🚀 DeepDrift Secure Relay v2.0 started")


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при остановке приложения"""
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
        "total_offline_messages": sum(offline_counts.values())
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

            # === ИНИЦИАЛИЗАЦИЯ ПОДКЛЮЧЕНИЯ ===
            if msg_type == "init":
                protocol_version = data.get("protocol_version", "1.0")
                
                # Проверка версии протокола
                if protocol_version != PROTOCOL_VERSION:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"Unsupported protocol version. Expected {PROTOCOL_VERSION}, got {protocol_version}"
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
                
                # Проверка аутентификации
                if provided_token and verify_auth_token(my_uid, provided_token):
                    is_authenticated = True
                else:
                    # Генерируем новый токен для нового пользователя
                    new_token = generate_auth_token(my_uid)
                    is_authenticated = True
                    await websocket.send_text(json.dumps({
                        "type": "auth_token",
                        "token": new_token,
                        "message": "Save this token for future connections"
                    }))
                
                # Регистрируем подключение
                active_connections[my_uid] = websocket
                logger.info(f"✅ UID {my_uid} connected (authenticated: {is_authenticated})")
                
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

            # === ПРОВЕРКА АУТЕНТИФИКАЦИИ ДЛЯ ОСТАЛЬНЫХ ОПЕРАЦИЙ ===
            if not is_authenticated or not my_uid:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Not authenticated"
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
                # Rate limiting проверка
                if is_rate_limited(my_uid):
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": "Rate limit exceeded. Please slow down."
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
                
                # Формируем payload для получателя
                payload = {
                    "type": "message",
                    "id": message_id,
                    "from_uid": my_uid,
                    "encrypted_payload": data.get("encrypted_payload"),
                    "fhrg_sig": data.get("fhrg_sig"),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                # Пытаемся доставить сообщение
                delivered = False
                if target_uid in active_connections:
                    try:
                        await active_connections[target_uid].send_text(json.dumps(payload))
                        delivered = True
                        logger.info(f"📨 Message {message_id} delivered {my_uid} -> {target_uid}")
                    except Exception as e:
                        logger.error(f"Failed to deliver message: {e}")
                        delivered = False
                
                # Если не доставлено, сохраняем офлайн
                if not delivered:
                    await save_offline_message(target_uid, payload)
                    logger.info(f"💾 Message {message_id} saved offline {my_uid} -> {target_uid}")
                
                # Отправляем ACK отправителю
                await websocket.send_text(json.dumps({
                    "type": "server_ack",
                    "id": message_id,
                    "status": "received",
                    "delivered_online": delivered,
                    "timestamp": datetime.utcnow().isoformat()
                }))
                
                # Отправляем статус "sent"
                await websocket.send_text(json.dumps({
                    "type": "status_update",
                    "id": message_id,
                    "status": "sent"
                }))
                
                continue
            
            # Неизвестный тип сообщения
            logger.warning(f"Unknown message type '{msg_type}' from {my_uid}")
            await websocket.send_text(json.dumps({
                "type": "error",
                "message": f"Unknown message type: {msg_type}"
            }))

    except WebSocketDisconnect:
        logger.info(f"❌ UID {my_uid} disconnected (normal)")
    except Exception as e:
        logger.error(f"❌ Unexpected error for {my_uid}: {e}", exc_info=True)
    finally:
        # Очистка подключения
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
