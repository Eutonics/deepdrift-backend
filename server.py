from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware  # <-- ВАЖНО
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()

# 1. РАЗРЕШАЕМ ВСЕ ПОДКЛЮЧЕНИЯ (Фикс 403 Forbidden)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешить всем
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

active_connections = {}

@app.get("/")
async def root():
    return {
        "status": "DeepDrift Relay v7 (CORS Fixed)", 
        "active_users_count": len(active_connections),
        "users_online": list(active_connections.keys())
    }

@app.websocket("/chat/{client_uid}")
async def websocket_endpoint(websocket: WebSocket, client_uid: str):
    # 2. ПРИНУДИТЕЛЬНОЕ ПРИНЯТИЕ СОЕДИНЕНИЯ
    await websocket.accept()
    
    # Кикаем старую сессию, если ID занят
    if client_uid in active_connections:
        try:
            await active_connections[client_uid].close()
            logger.info(f"♻️ Reconnecting session for {client_uid}")
        except:
            pass

    active_connections[client_uid] = websocket
    logger.info(f"✅ User {client_uid} CONNECTED")
    
    # Handshake
    try:
        await websocket.send_text(json.dumps({
            "type": "uid_assigned",
            "uid": client_uid,
            "timestamp": datetime.now().isoformat()
        }))
    except Exception as e:
        logger.error(f"❌ Handshake failed for {client_uid}: {e}")
        return
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            msg_type = data.get("type", "message")
            target = data.get("target_uid")
            
            # PING/PONG (Keep-Alive)
            if msg_type == "ping":
                 await websocket.send_text(json.dumps({"type": "pong"}))
                 continue

            if target and target in active_connections:
                target_ws = active_connections[target]
                
                # ... Логика пересылки (как была) ...
                if msg_type == "message":
                    await target_ws.send_text(json.dumps({
                        "type": "message",
                        "id": data.get("id"),
                        "from_uid": client_uid,
                        "encrypted_payload": data.get("encrypted_payload"),
                        "fhrg_sig": data.get("fhrg_sig"),
                        "timestamp": datetime.now().isoformat()
                    }))
                    
                    # Подтверждение отправки
                    await websocket.send_text(json.dumps({
                        "type": "status_update",
                        "id": data.get("id"),
                        "status": "sent"
                    }))
                    logger.info(f"📨 {client_uid} -> {target}")
                
                elif msg_type == "typing":
                     await target_ws.send_text(json.dumps({
                        "type": "typing",
                        "from_uid": client_uid
                    }))
                    
                elif msg_type == "delivery_receipt":
                    await target_ws.send_text(json.dumps({
                        "type": "status_update",
                        "id": data.get("message_id"),
                        "status": "delivered"
                    }))
            else:
                if msg_type == "message":
                    logger.warning(f"❌ {client_uid} -> {target} (Offline)")
                    await websocket.send_text(json.dumps({
                        "type": "message_failed",
                        "msg_id": data.get("id"),
                        "error": "User offline"
                    }))
                
    except WebSocketDisconnect:
        logger.info(f"🔴 User {client_uid} disconnected")
    except Exception as e:
        logger.error(f"⚠️ Error with {client_uid}: {e}")
    finally:
        if client_uid in active_connections and active_connections[client_uid] == websocket:
            del active_connections[client_uid]
