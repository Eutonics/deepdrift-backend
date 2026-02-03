from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json
import random
import datetime

app = FastAPI()
active_connections = {}

@app.get("/")
async def root():
    # Это нужно, чтобы ты мог просто открыть ссылку в браузере и "разбудить" сервер
    return {
        "status": "DeepDrift Relay Online",
        "time": datetime.datetime.now().isoformat(),
        "active_users": len(active_connections)
    }

@app.websocket("/chat")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    
    # 1. СРАЗУ генерируем временный UID и шлем его телефону
    # Это разорвет "мертвый замок"
    temp_uid = str(random.randint(100000, 999999))
    active_connections[temp_uid] = websocket
    
    await websocket.send_text(json.dumps({
        "type": "welcome",
        "my_uid": temp_uid,
        "message": "Connected to DeepDrift Cloud"
    }))
    
    print(f"🟢 User {temp_uid} connected")
    
    try:
        while True:
            # Слушаем сообщения
            raw_data = await websocket.receive_text()
            data = json.loads(raw_data)
            
            target = data.get("target_uid")
            payload = data.get("payload")
            
            if target in active_connections:
                await active_connections[target].send_text(json.dumps({
                    "from_uid": temp_uid,
                    "payload": payload,
                    "fhrg_sig": data.get("fhrg_sig")
                }))
            else:
                await websocket.send_text(json.dumps({
                    "type": "error", 
                    "message": f"Target {target} not found"
                }))
                
    except WebSocketDisconnect:
        if temp_uid in active_connections:
            del active_connections[temp_uid]
        print(f"🔴 User {temp_uid} disconnected")
