"""
Redis-сервис: подключение, pub/sub для горизонтального масштабирования.
"""
import asyncio
import json
import logging
from typing import Optional, Callable, Awaitable

import redis.asyncio as redis

from config import REDIS_URL

logger = logging.getLogger("DDChatRelay")


class RedisService:
    """Обёртка над redis.asyncio с поддержкой Pub/Sub."""

    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self._pubsub: Optional[redis.client.PubSub] = None
        self._pubsub_task: Optional[asyncio.Task] = None
        # callback: (channel: str, data: dict) -> None
        self._message_callback: Optional[Callable[[str, dict], Awaitable[None]]] = None

    async def connect(self):
        if not REDIS_URL:
            logger.warning("⚠️ REDIS_URL not set. Running without Redis.")
            return
        try:
            url = REDIS_URL.replace("cache://", "redis://")
            self.client = redis.from_url(
                url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=5.0,
                retry_on_timeout=True,
            )
            await self.client.ping()
            logger.info("✅ Redis connected successfully!")
        except Exception as e:
            logger.error(f"❌ Redis Connection Failed: {e}")
            self.client = None

    async def disconnect(self):
        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.close()
        if self.client:
            await self.client.close()

    @property
    def available(self) -> bool:
        return self.client is not None

    # ── Pub/Sub для горизонтального масштабирования ──────────────────────────
    # Каждый инстанс сервера подписывается на каналы вида `relay:{uid}`.
    # Когда получатель на другом инстансе — сообщение публикуется в Redis,
    # и инстанс с нужным WebSocket его доставляет.

    async def start_pubsub(self, callback: Callable[[str, dict], Awaitable[None]]):
        """Запускает подписчика. callback(channel, data) вызывается при получении."""
        if not self.client:
            return
        self._message_callback = callback
        self._pubsub = self.client.pubsub()
        # Подписываемся на паттерн relay:* — все сообщения для всех uid
        await self._pubsub.psubscribe("relay:*")
        self._pubsub_task = asyncio.create_task(self._pubsub_listener())
        logger.info("✅ Redis Pub/Sub listener started")

    async def _pubsub_listener(self):
        try:
            async for message in self._pubsub.listen():
                if message["type"] != "pmessage":
                    continue
                channel = message["channel"]
                try:
                    data = json.loads(message["data"])
                    if self._message_callback:
                        await self._message_callback(channel, data)
                except Exception as e:
                    logger.error(f"❌ Pub/Sub parse error: {e}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"❌ Pub/Sub listener error: {e}")

    async def publish(self, uid: str, payload: dict):
        """Публикует сообщение для uid через Redis Pub/Sub."""
        if not self.client:
            return
        try:
            await self.client.publish(f"relay:{uid}", json.dumps(payload))
        except Exception as e:
            logger.error(f"❌ Pub/Sub publish error: {e}")
