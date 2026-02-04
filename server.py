from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import random
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI()
active_connections = {}

@app.get("/")
async def root():
    return {
        "status": "DeepDrift Online",
        "active_users": len(active_connections),
        "timestamp": datetime.now().isoformat()
    }

@app.get("/stats")
async def stats():
    return {
        "active_connections": len(active_connections),
        "active_uids": list(active_connections.keys())
    }

@app.websocket("/{full_path:path}")
async def websocket_endpoint(websocket: WebSocket, full_path: str):
    await websocket.accept()
    
    # Generate Session UID
    uid = str(random.randint(1000, 9999))  # 4-digit для совместимости
    active_connections[uid] = websocket
    
    # Handshake: Send UID to client
    welcome_packet = {
        "type": "uid_assigned",
        "uid": uid,
        "timestamp": datetime.now().isoformat()
    }
    
    await websocket.send_text(json.dumps(welcome_packet))
    logger.info(f"✅ User {uid} connected via /{full_path}")
    
    try:
        while True:
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            msg_type = data.get("type", "message")
            
            if msg_type == "message":
                target = data.get("target_uid")
                payload = data.get("encrypted_payload")
                fhrg_sig = data.get("fhrg_sig")
                msg_id = data.get("msg_id")  # Client-generated message ID
                
                if target in active_connections:
                    # Relay message to target
                    await active_connections[target].send_text(json.dumps({
                        "type": "message",
                        "from_uid": uid,
                        "encrypted_payload": payload,
                        "fhrg_sig": fhrg_sig,
                        "timestamp": datetime.now().isoformat()
                    }))
                    
                    # Send delivery confirmation to sender
                    await websocket.send_text(json.dumps({
                        "type": "message_delivered",
                        "msg_id": msg_id,
                        "target_uid": target,
                        "timestamp": datetime.now().isoformat()
                    }))
                    
                    logger.info(f"📨 Route: {uid} -> {target} (msg_id: {msg_id})")
                else:
                    # Target offline - send error
                    await websocket.send_text(json.dumps({
                        "type": "message_failed",
                        "msg_id": msg_id,
                        "error": "User is offline",
                        "target_uid": target
                    }))
                    logger.info(f"❌ Failed: {uid} -> {target} (offline)")
            
            elif msg_type == "typing":
                # Forward typing indicator
                target = data.get("target_uid")
                if target in active_connections:
                    await active_connections[target].send_text(json.dumps({
                        "type": "typing",
                        "from_uid": uid,
                        "is_typing": data.get("is_typing", True)
                    }))
            
            elif msg_type == "ping":
                # Keep-alive response
                await websocket.send_text(json.dumps({
                    "type": "pong",
                    "timestamp": datetime.now().isoformat()
                }))
                
    except WebSocketDisconnect:
        logger.info(f"🔴 User {uid} disconnected normally")
    except Exception as e:
        logger.info(f"🔴 Connection closed for {uid}: {e}")
    finally:
        if uid in active_connections:
            del active_connections[uid]
        logger.info(f"🧹 Cleanup: {uid} removed. Active: {len(active_connections)}")
