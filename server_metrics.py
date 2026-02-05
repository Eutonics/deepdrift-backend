"""
Модуль для мониторинга сервера с помощью Prometheus метрик
Использование:
    from server_metrics import init_metrics, track_message, track_connection
"""

from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Response
import time

# ============================================================================
# МЕТРИКИ
# ============================================================================

# Счетчики
messages_sent_total = Counter(
    'deepdrift_messages_sent_total',
    'Total number of messages sent through the relay'
)

messages_delivered_total = Counter(
    'deepdrift_messages_delivered_total',
    'Total number of messages successfully delivered to recipients'
)

messages_offline_total = Counter(
    'deepdrift_messages_offline_total',
    'Total number of messages queued for offline delivery'
)

connections_total = Counter(
    'deepdrift_connections_total',
    'Total number of WebSocket connections established'
)

disconnections_total = Counter(
    'deepdrift_disconnections_total',
    'Total number of WebSocket disconnections'
)

errors_total = Counter(
    'deepdrift_errors_total',
    'Total number of errors',
    ['error_type']
)

rate_limits_total = Counter(
    'deepdrift_rate_limits_total',
    'Total number of rate limit hits'
)

# Gauges
active_connections_gauge = Gauge(
    'deepdrift_active_connections',
    'Number of currently active WebSocket connections'
)

offline_messages_gauge = Gauge(
    'deepdrift_offline_messages',
    'Number of messages currently queued offline'
)

# Histograms
message_latency_seconds = Histogram(
    'deepdrift_message_latency_seconds',
    'Message processing latency in seconds',
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
)

websocket_duration_seconds = Histogram(
    'deepdrift_websocket_duration_seconds',
    'WebSocket connection duration in seconds',
    buckets=[1, 5, 10, 30, 60, 300, 600, 3600, 7200]
)

# ============================================================================
# HELPER ФУНКЦИИ
# ============================================================================

def init_metrics(app):
    """
    Инициализирует endpoint для Prometheus метрик
    
    Args:
        app: FastAPI application instance
    """
    @app.get("/metrics")
    async def metrics():
        """Prometheus metrics endpoint"""
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST
        )

def track_message_sent(delivered_online: bool):
    """
    Отслеживает отправку сообщения
    
    Args:
        delivered_online: True если сообщение доставлено онлайн
    """
    messages_sent_total.inc()
    if delivered_online:
        messages_delivered_total.inc()
    else:
        messages_offline_total.inc()

def track_connection():
    """Отслеживает новое подключение"""
    connections_total.inc()
    active_connections_gauge.inc()

def track_disconnection():
    """Отслеживает отключение"""
    disconnections_total.inc()
    active_connections_gauge.dec()

def track_error(error_type: str):
    """
    Отслеживает ошибку
    
    Args:
        error_type: Тип ошибки (json_decode, websocket, etc.)
    """
    errors_total.labels(error_type=error_type).inc()

def track_rate_limit():
    """Отслеживает срабатывание rate limit"""
    rate_limits_total.inc()

def update_offline_queue_size(size: int):
    """
    Обновляет размер оффлайн очереди
    
    Args:
        size: Текущий размер очереди
    """
    offline_messages_gauge.set(size)

class MessageTimer:
    """Context manager для измерения времени обработки сообщения"""
    
    def __init__(self):
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            message_latency_seconds.observe(duration)

class ConnectionTimer:
    """Context manager для измерения длительности подключения"""
    
    def __init__(self):
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            websocket_duration_seconds.observe(duration)

# ============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ
# ============================================================================

"""
В server.py:

from server_metrics import (
    init_metrics,
    track_message_sent,
    track_connection,
    track_disconnection,
    track_error,
    track_rate_limit,
    MessageTimer,
    ConnectionTimer
)

# При старте приложения
init_metrics(app)

# При новом подключении
track_connection()
with ConnectionTimer():
    # ... код обработки подключения ...
    pass

# При отключении
track_disconnection()

# При отправке сообщения
with MessageTimer():
    # ... код отправки сообщения ...
    delivered = send_message(...)
    track_message_sent(delivered_online=delivered)

# При ошибке
try:
    data = json.loads(raw)
except json.JSONDecodeError:
    track_error('json_decode')

# При rate limit
if is_rate_limited(uid):
    track_rate_limit()
"""
