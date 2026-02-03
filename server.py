from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()
# Хранилище активных сессий: { "UID": WebSocket }
active_connections = {}

@app.get("/")
async def root():
    return {"status": "DeepDrift Relay v4 Online", "active_users": len(active_connections)}

@app.websocket("/ws/{my_uid}")
async def websocket_endpoint(websocket: WebSocket, my_uid: str):
    # Проверка на дубликат подключения
    if my_uid in active_connections:
        await websocket.accept()
        await websocket.send_text(json.dumps({"type": "error", "error": "This ID is already online"}))
        await websocket.close()
        return

    await websocket.accept()
    active_connections[my_uid] = websocket
    logger.info(f"✅ User {my_uid} is ONLINE")

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
                    "payload": payload,
                    "fhrg_sig": fhrg_sig
                }))
                logger.info(f"📨 {my_uid} -> {target}")
            else:
                await websocket.send_text(json.dumps({
                    "type": "error", 
                    "error": f"User {target} is offline",
                    "target_uid": target
                }))
    except:
        if my_uid in active_connections:
            del active_connections[my_uid]
        logger.info(f"🔴 User {my_uid} went offline")
