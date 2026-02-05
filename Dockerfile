FROM python:3.11-slim

WORKDIR /app

# Установка зависимостей системы
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Копируем файл зависимостей
COPY requirements_improved.txt requirements.txt

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код приложения
COPY server_improved.py server.py
COPY server_metrics.py server_metrics.py

# Открываем порт
EXPOSE 8000

# Запускаем приложение
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
