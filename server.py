from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RelayServer")

app = FastAPI()

# Хранилище активных соединений: { "UID": WebSocket }
active_connections = {}

@app.get("/")
async def health_check():
    return {"status": "DeepDrift Relay Online", "users_online": len(active_connections)}

@app.websocket("/ws/{my_uid}")
async def websocket_endpoint(websocket: WebSocket, my_uid: str):
    await websocket.accept()
    active_connections[my_uid] = websocket
    logger.info(f"🟢 User {my_uid} connected. Total: {len(active_connections)}")
    
    try:
        while True:
            # Ожидаем JSON: {"target_uid": "654321", "payload": "...", "fhrg_sig": "..."}
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            target_uid = data.get("target_uid")
            
            if target_uid in active_connections:
                # Пересылаем сообщение целиком (включая подпись и шифр)
                await active_connections[target_uid].send_text(json.dumps({
                    "from_uid": my_uid,
                    "payload": data.get("payload"),
                    "fhrg_sig": data.get("fhrg_sig")
                }))
                logger.info(f"📨 {my_uid} -> {target_uid}")
            else:
                await websocket.send_text(json.dumps({"error": "Target offline"}))
                
    except WebSocketDisconnect:
        if my_uid in active_connections:
            del active_connections[my_uid]
        logger.info(f"🔴 User {my_uid} disconnected.")
    except Exception as e:
        logger.error(f"⚠️ Error with {my_uid}: {e}")
