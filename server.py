from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import json

app = FastAPI()
active_connections = {}

@app.get("/")
async def root():
    return {"status": "DeepDrift Relay Online"}

# Сделали путь /chat, как в твоем приложении на скриншоте
@app.websocket("/chat")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    # Ждем первое сообщение от клиента с его UID
    try:
        initial_data = await websocket.receive_text()
        data = json.loads(initial_data)
        my_uid = data.get("my_uid", "unknown")
        
        active_connections[my_uid] = websocket
        print(f"🟢 User {my_uid} connected via /chat")
        
        while True:
            raw_data = await websocket.receive_text()
            msg_data = json.loads(raw_data)
            target = msg_data.get("target_uid")
            
            if target in active_connections:
                await active_connections[target].send_text(json.dumps({
                    "from_uid": my_uid,
                    "payload": msg_data.get("payload"),
                    "fhrg_sig": msg_data.get("fhrg_sig")
                }))
            else:
                await websocket.send_text(json.dumps({"error": "Target offline"}))
                
    except WebSocketDisconnect:
        # Убираем из списка при дисконнекте
        for uid, ws in list(active_connections.items()):
            if ws == websocket:
                del active_connections[uid]
                print(f"🔴 User {uid} disconnected")
