from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()
active_connections = {}
# НОВОЕ: Хранилище оффлайн сообщений { "UID": [список_пакетов] }
offline_inbox = {}

@app.get("/")
async def root():
    return {"status": "DeepDrift Relay v8", "online": list(active_connections.keys())}

@app.websocket("/chat/{client_uid}")
async def websocket_endpoint(websocket: WebSocket, client_uid: str):
    await websocket.accept()
    active_connections[client_uid] = websocket
    logger.info(f"✅ User {client_uid} ONLINE")
    
    # 1. ПРОВЕРЯЕМ ПОЧТОВЫЙ ЯЩИК
    if client_uid in offline_inbox and offline_inbox[client_uid]:
        logger.info(f"📦 Delivering {len(offline_inbox[client_uid])} pending messages to {client_uid}")
        for pending_msg in offline_inbox[client_uid]:
            await websocket.send_text(json.dumps(pending_msg))
        offline_inbox[client_uid] = [] # Очищаем ящик после доставки

    await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": client_uid}))
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            msg_type = data.get("type", "message")
            target = data.get("target_uid")
            
            if target in active_connections:
                # Юзер онлайн - шлем как обычно
                await active_connections[target].send_text(json.dumps({
                    "type": "message",
                    "id": data.get("id"),
                    "from_uid": client_uid,
                    "encrypted_payload": data.get("encrypted_payload"),
                    "fhrg_sig": data.get("fhrg_sig")
                }))
                # Шлем статус "sent" отправителю
                await websocket.send_text(json.dumps({"type": "status_update", "id": data.get("id"), "status": "sent"}))
            else:
                # ЮЗЕР ОФФЛАЙН - КЛАДЕМ В ЯЩИК
                if msg_type == "message":
                    if target not in offline_inbox: offline_inbox[target] = []
                    offline_inbox[target].append({
                        "type": "message",
                        "id": data.get("id"),
                        "from_uid": client_uid,
                        "encrypted_payload": data.get("encrypted_payload"),
                        "fhrg_sig": data.get("fhrg_sig")
                    })
                    logger.info(f"📥 Saved to inbox: {client_uid} -> {target}")
                    # Сообщаем отправителю, что сохранили (delivered на сервер)
                    await websocket.send_text(json.dumps({"type": "status_update", "id": data.get("id"), "status": "sent"}))

    except Exception:
        if client_uid in active_connections: del active_connections[client_uid]
