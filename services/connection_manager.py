"""
Менеджер соединений: локальный реестр WebSocket'ов + Redis Pub/Sub fallback.
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, Optional

from fastapi import WebSocket

from config import WS_PING_INTERVAL

logger = logging.getLogger("DDChatRelay")


class ConnectionManager:
    """
    Управляет активными WebSocket-соединениями.

    При горизонтальном масштабировании:
    - Каждый инстанс хранит только свои соединения.
    - Если получатель не найден локально — сообщение публикуется в Redis Pub/Sub.
    - Инстанс, на котором сидит получатель, ловит сообщение и доставляет.
    """

    def __init__(self):
        self._connections: Dict[str, WebSocket] = {}
        self._lock = asyncio.Lock()
        self._redis_service = None  # Установится позже

    def set_redis(self, redis_service):
        self._redis_service = redis_service

    @property
    def online_count(self) -> int:
        return len(self._connections)

    def is_online(self, uid: str) -> bool:
        return uid in self._connections

    async def register(self, uid: str, ws: WebSocket):
        async with self._lock:
            # Если было старое соединение — молча вытесняем его
            self._connections[uid] = ws
        logger.info(f"✅ {uid} connected (total: {self.online_count})")

    async def unregister(self, uid: str, ws: WebSocket):
        async with self._lock:
            # Удаляем только если это именно тот WebSocket, который отключился.
            # Защита от race condition: новое соединение могло уже зарегистрироваться
            # под тем же uid до того как старое успело разрегистрироваться.
            if uid in self._connections and self._connections[uid] is ws:
                self._connections.pop(uid, None)

    async def send_to(self, ws: WebSocket, payload: dict) -> bool:
        """Отправляет JSON в WebSocket. Возвращает True при успехе."""
        try:
            # Проверяем оба состояния: клиентское и серверное
            if ws.client_state.value != 1:  # WebSocketState.CONNECTED == 1
                return False
            if ws.application_state.value != 1:
                return False
            await ws.send_text(json.dumps(payload))
            return True
        except Exception as e:
            logger.warning(f"⚠️ Socket write error: {type(e).__name__}: {e}")
            return False

    async def deliver_to_uid(self, uid: str, payload: dict) -> bool:
        """
        Пытается доставить сообщение пользователю.
        1. Ищет локально.
        2. Если не нашёл — публикует через Redis Pub/Sub.
        Возвращает True если доставлено локально.
        """
        if uid in self._connections:
            ws = self._connections[uid]
            delivered = await self.send_to(ws, payload)
            if not delivered:
                logger.warning(f"🔌 Removing dead connection for {uid}")
                async with self._lock:
                    if uid in self._connections and self._connections[uid] is ws:
                        del self._connections[uid]
                # Попробуем через Pub/Sub — вдруг на другом инстансе
                if self._redis_service and self._redis_service.available:
                    await self._redis_service.publish(uid, payload)
                return False
            return True
        else:
            # Пользователь не на этом инстансе — шлём через Pub/Sub
            if self._redis_service and self._redis_service.available:
                await self._redis_service.publish(uid, payload)
            return False

    async def handle_pubsub_message(self, channel: str, data: dict):
        """Callback для Redis Pub/Sub — доставляет сообщение, если uid здесь."""
        # channel = "relay:{uid}"
        uid = channel.split(":", 1)[1] if ":" in channel else None
        if uid and uid in self._connections:
            await self.send_to(self._connections[uid], data)

    def get_ws(self, uid: str) -> Optional[WebSocket]:
        return self._connections.get(uid)
