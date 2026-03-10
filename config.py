"""
Конфигурация DeepDrift Backend.
Все переменные окружения и константы — в одном месте.
"""
import os
import re

# ─── Приложение ─────────────────────────────────────────────────────────────
APP_TITLE   = "DeepDrift Secure Relay"
APP_VERSION = "6.0.0"

# ─── Regex / UID ────────────────────────────────────────────────────────────
UID_PATTERN = re.compile(r"^\d{6}$")  # UID — строго 6 цифр

# ─── Redis ──────────────────────────────────────────────────────────────────
REDIS_URL = os.environ.get("REDIS_URL")

# ─── Firebase ───────────────────────────────────────────────────────────────
FB_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

# ─── Cloudflare R2 ─────────────────────────────────────────────────────────
R2_ENDPOINT = os.environ.get("R2_ENDPOINT_URL", "")
R2_KEY_ID   = os.environ.get("R2_ACCESS_KEY_ID", "")
R2_SECRET   = os.environ.get("R2_SECRET_KEY", "")
R2_BUCKET   = os.environ.get("R2_BUCKET_NAME", "ddchat-files")

# ─── CORS ───────────────────────────────────────────────────────────────────
# В продакшене — ваши домены; для разработки можно ["*"]
CORS_ORIGINS = os.environ.get("CORS_ORIGINS", "").split(",") if os.environ.get("CORS_ORIGINS") else ["*"]

# ─── Auth ───────────────────────────────────────────────────────────────────
NONCE_TTL_SECONDS = 60
NONCE_SIZE_BYTES  = 32

# ─── Rate limit ─────────────────────────────────────────────────────────────
RATE_LIMIT_MAX    = 60
RATE_LIMIT_WINDOW = 60   # секунд

# ─── Upload ─────────────────────────────────────────────────────────────────
MAX_UPLOAD_SIZE     = 150 * 1024 * 1024   # 150 MB
UPLOAD_TOKEN_TTL    = 24 * 3600           # 24 часа
UPLOAD_DIR          = "uploads"

# ─── WebSocket ──────────────────────────────────────────────────────────────
WS_MAX_MESSAGE_SIZE = 1 * 1024 * 1024     # 1 MB — максимальный размер WS-сообщения
WS_PING_INTERVAL    = 30                  # секунд

# ─── Offline queue ──────────────────────────────────────────────────────────
OFFLINE_MSG_TTL     = 7 * 24 * 3600       # 7 дней

# ─── Group keys ─────────────────────────────────────────────────────────────
GROUP_KEY_TTL       = 90 * 24 * 3600      # 90 дней

# ─── Rate limit memory cleanup ──────────────────────────────────────────────
RATE_LIMIT_MEMORY_MAX_KEYS = 10000        # Максимум ключей в fallback rate limiter
