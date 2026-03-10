"""
Офлайн-очередь сообщений: сохранение и доставка при подключении.
"""
import json
import logging
import asyncio
from typing import Optional

from fastapi import WebSocket

from config import OFFLINE_MSG_TTL

logger = logging.getLogger("DDChatRelay")


class OfflineQueue:
    """Управляет офлайн-очередью сообщений в Redis."""

    def __init__(self, connection_manager):
        self._cm = connection_manager

    async def store(self, redis_client, target_uid: str, message_data: dict):
        """Сохраняет сообщение в офлайн-очередь с дедупликацией."""
        if not redis_client:
            return
        try:
            from_uid   = message_data.get("from_uid", "unknown")
            message_id = message_data.get("id")

            # Дедупликация по message_id
            if message_id:
                dedup_key = f"offline_id:{target_uid}:{message_id}"
                if await redis_client.exists(dedup_key):
                    logger.debug(f"🔁 Dedup: skipping already-queued msg {message_id} for {target_uid}")
                    return
                await redis_client.setex(dedup_key, OFFLINE_MSG_TTL, "1")

            msg_json = json.dumps(message_data)

            # Глобальная очередь
            offline_key_global = f"offline_queue:{target_uid}"
            await redis_client.rpush(offline_key_global, msg_json)
            await redis_client.expire(offline_key_global, OFFLINE_MSG_TTL)

            # Per-sender очередь
            offline_key_specific = f"offline:{target_uid}:from:{from_uid}"
            await redis_client.rpush(offline_key_specific, msg_json)
            await redis_client.expire(offline_key_specific, OFFLINE_MSG_TTL)

            # Группы: дублируем под ключом group_id
            group_id = message_data.get("group_id")
            if group_id and group_id != from_uid:
                offline_key_group = f"offline:{target_uid}:from:{group_id}"
                await redis_client.rpush(offline_key_group, msg_json)
                await redis_client.expire(offline_key_group, OFFLINE_MSG_TTL)
        except Exception as e:
            logger.error(f"❌ Failed to store offline message: {e}")

    async def send_all(self, websocket: WebSocket, my_uid: str, redis_client):
        """Отправляет все офлайн-сообщения при подключении."""
        if not redis_client:
            return
        await asyncio.sleep(0.5)
        try:
            offline_key = f"offline_queue:{my_uid}"
            messages = await redis_client.lrange(offline_key, 0, -1)
            if messages:
                logger.info(f"📬 Sending {len(messages)} global offline messages to {my_uid}")
                success_count = 0
                for msg_json in messages:
                    if await self._cm.send_to(websocket, json.loads(msg_json)):
                        success_count += 1
                    else:
                        break
                if success_count > 0:
                    await redis_client.ltrim(offline_key, success_count, -1)
        except Exception as e:
            logger.error(f"❌ Error sending offline messages: {e}")

    async def send_from(self, websocket: WebSocket, my_uid: str, from_uid: str, redis_client):
        """Отправляет офлайн-сообщения от конкретного отправителя."""
        if not redis_client:
            return
        try:
            offline_key = f"offline:{my_uid}:from:{from_uid}"
            messages = await redis_client.lrange(offline_key, 0, -1)
            if messages:
                logger.info(f"📬 Sending {len(messages)} messages from {from_uid} to {my_uid}")
                success_count = 0
                for msg_json in messages:
                    if await self._cm.send_to(websocket, json.loads(msg_json)):
                        success_count += 1
                    else:
                        break
                if success_count > 0:
                    await redis_client.ltrim(offline_key, success_count, -1)
        except Exception as e:
            logger.error(f"❌ Error sending specific offline messages: {e}")
