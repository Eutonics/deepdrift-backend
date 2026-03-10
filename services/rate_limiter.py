"""
Rate limiter: Redis sliding window + fallback in-memory с ограничением по памяти.
"""
import logging
import time
from datetime import datetime
from typing import Dict

from config import RATE_LIMIT_MAX, RATE_LIMIT_WINDOW, RATE_LIMIT_MEMORY_MAX_KEYS

logger = logging.getLogger("DDChatRelay")


class RateLimiter:
    """
    60 сообщений / 60 секунд per user.
    Redis — основной. In-memory — fallback с защитой от утечки памяти.
    """

    def __init__(self):
        self._memory_store: Dict[str, list] = {}

    def _check_memory(self, uid: str) -> bool:
        """Fallback rate limit без Redis."""
        # Защита от утечки памяти: если слишком много ключей — чистим старые
        if len(self._memory_store) > RATE_LIMIT_MEMORY_MAX_KEYS:
            self._cleanup_memory()

        now = datetime.now().timestamp()
        timestamps = self._memory_store.get(uid, [])
        timestamps = [t for t in timestamps if now - t < RATE_LIMIT_WINDOW]
        if len(timestamps) >= RATE_LIMIT_MAX:
            return False
        timestamps.append(now)
        self._memory_store[uid] = timestamps
        return True

    def _cleanup_memory(self):
        """Удаляет UID, у которых все записи старше RATE_LIMIT_WINDOW."""
        now = datetime.now().timestamp()
        stale_keys = [
            uid for uid, ts in self._memory_store.items()
            if not ts or all(now - t >= RATE_LIMIT_WINDOW for t in ts)
        ]
        for key in stale_keys:
            del self._memory_store[key]
        logger.info(f"🧹 Rate limit memory cleanup: removed {len(stale_keys)} stale keys")

    async def check(self, uid: str, redis_client) -> bool:
        """
        Проверяет rate limit.
        Redis sliding-window если доступен, иначе fallback в память.
        """
        if not redis_client:
            return self._check_memory(uid)
        try:
            now    = time.time()
            key    = f"rate:{uid}"
            cutoff = now - RATE_LIMIT_WINDOW
            async with redis_client.pipeline(transaction=True) as pipe:
                pipe.zremrangebyscore(key, 0, cutoff)
                pipe.zadd(key, {str(now): now})
                pipe.zcard(key)
                pipe.expire(key, RATE_LIMIT_WINDOW)
                results = await pipe.execute()
            count = results[2]
            return count <= RATE_LIMIT_MAX
        except Exception:
            return self._check_memory(uid)

    def clean(self, uid: str):
        """Очищает записи пользователя при disconnect."""
        self._memory_store.pop(uid, None)
