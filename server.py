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
    
    # Регистрация (Handshake)
    uid = str(random.randint(100000, 999999))
    active_connections[uid] = websocket
    
    await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": uid}))
    logger.info(f"✅ User {uid} connected")
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            target = data.get("target_uid")
            payload = data.get("encrypted_payload") # Это наш зашифрованный текст
            fhrg_sig = data.get("fhrg_sig")        # Это наша фрактальная подпись
            
            if target in active_connections:
                # Шлем пакет получателю
                await active_connections[target].send_text(json.dumps({
                    "type": "message",
                    "from_uid": uid,
                    "encrypted_payload": payload,
                    "fhrg_sig": fhrg_sig
                }))
                
                # ВОТ ЗДЕСЬ МАГИЯ: Логируем то, что видит сервер
                logger.info(f"📨 ROUTE: {uid} -> {target}")
                logger.info(f"📦 RAW DATA ON SERVER: {payload[:50]}...") 
                logger.info(f"🧠 FHRG SIGNATURE: {fhrg_sig}")
            else:
                await websocket.send_text(json.dumps({"type": "error", "error": "Offline"}))
    except:
        if uid in active_connections:
            del active_connections[uid]
