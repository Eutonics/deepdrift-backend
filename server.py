from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()
active_connections = {}

@app.get("/")
async def root():
    # Показываем, кто реально сейчас подключен
    return {
        "status": "DeepDrift Relay v6", 
        "active_users_count": len(active_connections),
        "users_online": list(active_connections.keys())
    }

# ВАЖНОЕ ИЗМЕНЕНИЕ: Мы явно ловим client_uid из URL
@app.websocket("/chat/{client_uid}")
async def websocket_endpoint(websocket: WebSocket, client_uid: str):
    await websocket.accept()
    
    # Если такой ID уже есть - отключаем старого (перехват сессии)
    if client_uid in active_connections:
        try:
            await active_connections[client_uid].close()
            logger.info(f"♻️ Replaced existing session for {client_uid}")
        except:
            pass

    # ИСПОЛЬЗУЕМ ID, КОТОРЫЙ ПРИСЛАЛ ТЕЛЕФОН
    active_connections[client_uid] = websocket
    
    logger.info(f"✅ User {client_uid} connected manually")
    
    # Подтверждаем клиенту его же ID (чтобы снять спиннер загрузки)
    await websocket.send_text(json.dumps({
        "type": "uid_assigned",
        "uid": client_uid,
        "timestamp": datetime.now().isoformat()
    }))
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            msg_type = data.get("type", "message")
            target = data.get("target_uid")
            
            # PING/PONG для поддержания связи
            if msg_type == "ping":
                 await websocket.send_text(json.dumps({"type": "pong"}))
                 continue

            if target and target in active_connections:
                target_ws = active_connections[target]
                
                if msg_type == "message":
                    # Пересылаем сообщение
                    await target_ws.send_text(json.dumps({
                        "type": "message",
                        "id": data.get("id"),
                        "from_uid": client_uid, # От кого реально пришло
                        "encrypted_payload": data.get("encrypted_payload"),
                        "fhrg_sig": data.get("fhrg_sig"),
                        "timestamp": datetime.now().isoformat()
                    }))
                    
                    # Шлем галочку "Sent" отправителю
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
                    # Подтверждение доставки
                    await target_ws.send_text(json.dumps({
                        "type": "status_update",
                        "id": data.get("message_id"),
                        "status": "delivered"
                    }))
                    
            else:
                # Если получателя нет в списке
                if msg_type == "message":
                    logger.warning(f"❌ {client_uid} -> {target} (Target Offline)")
                    # Можно отправить ошибку клиенту, но лучше просто промолчать или сохранить в очередь (в будущем)
                    # Пока шлем ошибку, чтобы ты видел в дебаге
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
