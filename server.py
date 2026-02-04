from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
import asyncio
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

active_connections = {}
offline_inbox = {}

@app.get("/")
async def health():
    return {"status": "ONLINE v11", "users": list(active_connections.keys())}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid = None
    
    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            if msg_type == "init":
                my_uid = data.get("my_uid")
                active_connections[my_uid] = websocket
                logger.info(f"✅ UID {my_uid} Online")
                await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": my_uid}))
                
                # СРАЗУ отдаем оффлайн сообщения
                if my_uid in offline_inbox and offline_inbox[my_uid]:
                    for msg in offline_inbox[my_uid]:
                        await websocket.send_text(json.dumps(msg))
                    offline_inbox[my_uid] = []
                continue

            if msg_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
                continue

            if msg_type == "delivery_receipt":
                target = data.get("target_uid")
                if target in active_connections:
                    await active_connections[target].send_text(json.dumps({
                        "type": "status_update",
                        "id": data.get("message_id"),
                        "status": "delivered"
                    }))
                continue

            # Роутинг сообщений
            target_uid = data.get("target_uid")
            if not target_uid or not my_uid: continue

            payload = {
                "type": "message",
                "id": data.get("id"),
                "from_uid": my_uid,
                "encrypted_payload": data.get("encrypted_payload"),
                "fhrg_sig": data.get("fhrg_sig"),
                "timestamp": datetime.utcnow().isoformat()
            }

            if target_uid in active_connections:
                try:
                    await active_connections[target_uid].send_text(json.dumps(payload))
                except:
                    offline_inbox.setdefault(target_uid, []).append(payload)
            else:
                offline_inbox.setdefault(target_uid, []).append(payload)

            # Отклик отправителю (одна галочка)
            await websocket.send_text(json.dumps({"type": "status_update", "id": data.get("id"), "status": "sent"}))

    except Exception:
        if my_uid and active_connections.get(my_uid) is websocket:
            del active_connections[my_uid]
