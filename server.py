from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()

# Разрешаем CORS (для браузеров и веб-демок)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальное хранилище
active_connections = {} # { "UID": WebSocket }
offline_inbox = {}      # { "UID": [messages] }

@app.get("/")
async def root():
    return {
        "status": "DeepDrift Relay v9 Online",
        "online_users": list(active_connections.keys()),
        "inbox_sizes": {uid: len(msgs) for uid, msgs in offline_inbox.items() if msgs}
    }

# МАРШРУТ /ws (именно его ищет твой лог!)
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Принимаем соединение БЕЗ проверки Origin (убирает 403)
    await websocket.accept()
    my_uid = "unknown"
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            # --- 1. РЕГИСТРАЦИЯ ---
            if data.get("type") == "init" or data.get("type") == "uid_assigned":
                my_uid = data.get("my_uid") or data.get("uid")
                if not my_uid: continue
                
                active_connections[my_uid] = websocket
                logger.info(f"✅ User {my_uid} authenticated on /ws")
                
                # Подтверждаем клиенту
                await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": my_uid}))
                
                # Доставляем оффлайн почту
                if my_uid in offline_inbox and offline_inbox[my_uid]:
                    logger.info(f"📦 Emptying inbox for {my_uid}")
                    for msg in offline_inbox[my_uid]:
                        await websocket.send_text(json.dumps(msg))
                    offline_inbox[my_uid] = []
                continue

            # --- 2. ПЕРЕСЫЛКА ---
            target_uid = data.get("target_uid")
            if not target_uid: continue

            # Формируем пакет
            payload = {
                "type": "message",
                "id": data.get("id", "no-id"),
                "from_uid": my_uid,
                "encrypted_payload": data.get("encrypted_payload"),
                "fhrg_sig": data.get("fhrg_sig"),
                "timestamp": datetime.now().isoformat()
            }

            if target_uid in active_connections:
                # Юзер онлайн
                await active_connections[target_uid].send_text(json.dumps(payload))
                # Шлем статус "доставлено на сервер" отправителю
                await websocket.send_text(json.dumps({"type": "status_update", "id": payload["id"], "status": "sent"}))
                logger.info(f"📨 {my_uid} -> {target_uid} (Live)")
            else:
                # Юзер оффлайн - в ящик
                if target_uid not in offline_inbox: offline_inbox[target_uid] = []
                offline_inbox[target_uid].append(payload)
                # Все равно шлем статус "sent" (сообщение на сервере)
                await websocket.send_text(json.dumps({"type": "status_update", "id": payload["id"], "status": "sent"}))
                logger.info(f"📥 {my_uid} -> {target_uid} (Stored in Inbox)")

    except WebSocketDisconnect:
        logger.info(f"🔴 User {my_uid} disconnected")
    except Exception as e:
        logger.error(f"⚠️ Error with {my_uid}: {e}")
    finally:
        if my_uid in active_connections:
            del active_connections[my_uid]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
