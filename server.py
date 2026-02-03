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
    return {"status": "DeepDrift Relay Online", "active_users": len(active_connections)}

@app.websocket("/{full_path:path}")
async def websocket_endpoint(websocket: WebSocket, full_path: str):
    await websocket.accept()
    
    # Генерация UID
    uid = str(random.randint(100000, 999999))
    active_connections[uid] = websocket
    
    # Пакет приветствия: Формат в точности как ждет мобильное приложение
    welcome_packet = {
        "type": "uid_assigned",
        "uid": uid
    }
    
    await websocket.send_text(json.dumps(welcome_packet))
    logger.info(f"✅ User {uid} connected via /{full_path}")
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            # Логика пересылки
            target = data.get("target_uid")
            payload = data.get("encrypted_payload")
            fhrg_sig = data.get("fhrg_sig")
            
            if target in active_connections:
                await active_connections[target].send_text(json.dumps({
                    "type": "message",
                    "from_uid": uid,
                    "encrypted_payload": payload,
                    "fhrg_sig": fhrg_sig
                }))
                logger.info(f"📨 Route: {uid} -> {target}")
            else:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "error": "Target user is offline"
                }))
    except Exception as e:
        logger.info(f"🔴 Connection closed for {uid}: {e}")
    finally:
        if uid in active_connections:
            del active_connections[uid]
