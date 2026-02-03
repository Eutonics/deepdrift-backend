"""
DeepDrift Secure - Real-Time E2E Encrypted Messenger Server
FastAPI WebSocket relay server with UID-based routing
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import json
import random
from datetime import datetime
from typing import Dict
import asyncio

app = FastAPI(title="DeepDrift Secure Server")

# CORS for web clients (optional)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connection Manager: UID -> WebSocket
connections: Dict[str, WebSocket] = {}


def generate_uid() -> str:
    """Generate unique 6-digit UID"""
    while True:
        uid = str(random.randint(100000, 999999))
        if uid not in connections:
            return uid


@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "DeepDrift Secure Server",
        "status": "online",
        "active_users": len(connections),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/stats")
async def stats():
    """Server statistics"""
    return {
        "active_connections": len(connections),
        "active_uids": list(connections.keys())
    }


@app.websocket("/chat")
async def websocket_endpoint(websocket: WebSocket):
    """Main WebSocket endpoint for messaging"""
    await websocket.accept()
    
    # Generate and assign UID
    uid = generate_uid()
    connections[uid] = websocket
    
    print(f"[{datetime.now()}] ✅ User {uid} connected. Active users: {len(connections)}")
    
    try:
        # Send UID to client
        await websocket.send_json({
            "type": "uid_assigned",
            "uid": uid,
            "timestamp": datetime.now().isoformat()
        })
        
        # Message loop
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            # Handle different message types
            msg_type = message.get("type", "message")
            
            if msg_type == "message":
                # Route encrypted message
                target_uid = message.get("target_uid")
                encrypted_payload = message.get("encrypted_payload")
                
                if not target_uid or not encrypted_payload:
                    await websocket.send_json({
                        "type": "error",
                        "error": "Missing target_uid or encrypted_payload"
                    })
                    continue
                
                # Check if target is online
                if target_uid not in connections:
                    await websocket.send_json({
                        "type": "error",
                        "error": f"User {target_uid} is offline",
                        "target_uid": target_uid
                    })
                    print(f"[{datetime.now()}] ❌ {uid} -> {target_uid}: Target offline")
                    continue
                
                # Forward encrypted message to target
                target_ws = connections[target_uid]
                try:
                    await target_ws.send_json({
                        "type": "message",
                        "from_uid": uid,
                        "encrypted_payload": encrypted_payload,
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    # Send delivery confirmation to sender
                    await websocket.send_json({
                        "type": "delivered",
                        "target_uid": target_uid,
                        "timestamp": datetime.now().isoformat()
                    })
                    
                    print(f"[{datetime.now()}] 📨 {uid} -> {target_uid}: Message delivered")
                    
                except Exception as e:
                    await websocket.send_json({
                        "type": "error",
                        "error": f"Failed to deliver to {target_uid}",
                        "details": str(e)
                    })
                    print(f"[{datetime.now()}] ❌ Delivery failed: {e}")
            
            elif msg_type == "ping":
                # Keep-alive
                await websocket.send_json({
                    "type": "pong",
                    "timestamp": datetime.now().isoformat()
                })
            
            else:
                await websocket.send_json({
                    "type": "error",
                    "error": f"Unknown message type: {msg_type}"
                })
    
    except WebSocketDisconnect:
        print(f"[{datetime.now()}] 👋 User {uid} disconnected")
    except Exception as e:
        print(f"[{datetime.now()}] ⚠️ Error for user {uid}: {e}")
    finally:
        # Cleanup
        if uid in connections:
            del connections[uid]
        print(f"[{datetime.now()}] 🧹 User {uid} removed. Active users: {len(connections)}")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 DeepDrift Secure Server Starting...")
    print("=" * 60)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
