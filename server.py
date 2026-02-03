from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import logging

# Настройка логов, чтобы видеть ошибки в панели Render
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()
active_connections = {}

@app.get("/")
async def root():
    return {
        "status": "DeepDrift Relay Online", 
        "connected_uids": list(active_connections.keys())
    }

@app.websocket("/chat/{my_uid}")
async def websocket_endpoint(websocket: WebSocket, my_uid: str):
    await websocket.accept()
    
    # Регистрируем пользователя
    active_connections[my_uid] = websocket
    logger.info(f"CONNECTED: {my_uid}")
    
    # Подтверждаем клиенту
    await websocket.send_text(json.dumps({
        "type": "uid_assigned",
        "uid": my_uid
    }))
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            target = data.get("target_uid")
            payload = data.get("encrypted_payload")
            fhrg_sig = data.get("fhrg_sig")
            
            if target in active_connections:
                await active_connections[target].send_text(json.dumps({
                    "type": "message",
                    "from_uid": my_uid,
                    "encrypted_payload": payload,
                    "fhrg_sig": fhrg_sig
                }))
                logger.info(f"FORWARD: {my_uid} -> {target}")
            else:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "error": f"User {target} is offline"
                }))
    except Exception as e:
        logger.info(f"DISCONNECT: {my_uid} (Error: {e})")
    finally:
        if my_uid in active_connections:
            del active_connections[my_uid]
