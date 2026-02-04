from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging

logging.basicConfig(level=logging.INFO)
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

@app.get("/")
async def root():
    return {"status": "DeepDrift Online", "users": list(active_connections.keys())}

# ТЕПЕРЬ ТОЛЬКО ОДИН ПРОСТОЙ ПУТЬ
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid = "unknown"
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            # Первый пакет от клиента должен быть типом 'init'
            if data.get("type") == "init":
                my_uid = data.get("my_uid")
                active_connections[my_uid] = websocket
                await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": my_uid}))
                logger.info(f"✅ User {my_uid} connected to /ws")
                continue

            # Логика пересылки
            target = data.get("target_uid")
            if target in active_connections:
                await active_connections[target].send_text(json.dumps({
                    "type": "message",
                    "from_uid": my_uid,
                    "encrypted_payload": data.get("encrypted_payload")
                }))
    except:
        if my_uid in active_connections:
            del active_connections[my_uid]
