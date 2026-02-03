from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()
active_connections = {}

@app.get("/")
async def root():
    return {"status": "DeepDrift Relay Online", "online_units": list(active_connections.keys())}

# Мы убрали генерацию рандомных чисел здесь. 
# Теперь ID берется из пути: /chat/1234
@app.websocket("/chat/{my_uid}")
async def websocket_endpoint(websocket: WebSocket, my_uid: str):
    await websocket.accept()
    
    active_connections[my_uid] = websocket
    logger.info(f"✅ User {my_uid} authenticated and online.")
    
    # Подтверждаем клиенту, что он в сети
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
                logger.info(f"📨 {my_uid} -> {target}")
            else:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "error": "Target user is offline",
                    "target_uid": target
                }))
    except Exception as e:
        logger.info(f"🔴 connection closed for {my_uid}")
    finally:
        if my_uid in active_connections:
            del active_connections[my_uid]
