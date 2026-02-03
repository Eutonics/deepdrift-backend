from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()
active_connections = {}

@app.get("/")
async def root():
    return {"status": "DeepDrift Online", "active_users": len(active_connections)}

@app.websocket("/{full_path:path}")
async def websocket_endpoint(websocket: WebSocket, full_path: str):
    await websocket.accept()
    
    # Генерация UID
    uid = str(random.randint(100000, 999999))
    active_connections[uid] = websocket
    
    # ВАЖНО: Шлем ровно то, что ждет home_screen.dart (тип uid_assigned)
    welcome_packet = {
        "type": "uid_assigned",
        "uid": uid
    }
    
    await websocket.send_text(json.dumps(welcome_packet))
    logger.info(f"✅ User {uid} assigned and connected.")
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            # Логика пересылки из chat_screen.dart
            target = data.get("target_uid")
            payload = data.get("encrypted_payload") # Ждем этот ключ
            
            if target in active_connections:
                await active_connections[target].send_text(json.dumps({
                    "type": "message",
                    "from_uid": uid,
                    "encrypted_payload": payload # Шлем этот ключ
                }))
                logger.info(f"📨 {uid} -> {target}")
            else:
                # Если цель офлайн, шлем ошибку как просит chat_screen.dart
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "error": "User is offline"
                }))
    except Exception as e:
        logger.info(f"🔴 Connection closed for {uid}: {e}")
    finally:
        if uid in active_connections:
            del active_connections[uid]
