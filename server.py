from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
from datetime import datetime
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("DeepDriftRelay")

app = FastAPI(title="DeepDrift Relay v10.1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

active_connections: Dict[str, WebSocket] = {}
offline_inbox: Dict[str, List[dict]] = {}

@app.get("/")
async def health():
    return {
        "status": "DeepDrift Relay v10.1 ONLINE",
        "online_users": list(active_connections.keys()),
        "offline_inbox": {k: len(v) for k, v in offline_inbox.items() if v}
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    my_uid: str | None = None
    logger.info("🔌 New WebSocket connection")

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            msg_type = data.get("type")

            # 1. АВТОРИЗАЦИЯ
            if msg_type in ("init", "uid_assigned"):
                uid = data.get("my_uid") or data.get("uid")
                if not uid: continue
                active_connections[uid] = websocket
                my_uid = uid
                logger.info(f"✅ UID {uid} bound to socket")
                await websocket.send_text(json.dumps({"type": "uid_assigned", "uid": uid}))

                if uid in offline_inbox and offline_inbox[uid]:
                    for msg in offline_inbox[uid]:
                        await websocket.send_text(json.dumps(msg))
                    offline_inbox[uid].clear()
                continue

            # 2. ПИНГ
            if msg_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "ts": datetime.utcnow().isoformat()}))
                continue

            # 3. ПОДТВЕРЖДЕНИЕ ДОСТАВКИ (Receipts)
            # Если телефон прислал квитанцию, пробрасываем её отправителю
            if msg_type == "delivery_receipt":
                target = data.get("target_uid")
                if target in active_connections:
                    await active_connections[target].send_text(json.dumps({
                        "type": "status_update",
                        "id": data.get("message_id"),
                        "status": "delivered"
                    }))
                continue

            # 4. МАРШРУТИЗАЦИЯ СООБЩЕНИЙ
            target_uid = data.get("target_uid")
            if not target_uid or not my_uid: continue

            payload = {
                "type": "message",
                "id": data.get("id"),
                "from_uid": my_uid,
                "encrypted_payload": data.get("encrypted_payload"),
                "fhrg_sig": data.get("fhrg_sig"),
                "timestamp": datetime.utcnow().isoformat()
            }

            if target_uid in active_connections:
                try:
                    await active_connections[target_uid].send_text(json.dumps(payload))
                    logger.info(f"📨 {my_uid} → {target_uid} (LIVE)")
                except:
                    offline_inbox.setdefault(target_uid, []).append(payload)
            else:
                offline_inbox.setdefault(target_uid, []).append(payload)
                logger.info(f"📥 {my_uid} → {target_uid} (OFFLINE)")

            # Отправляем отправителю статус "Ушло на сервер" (Первая галочка)
            await websocket.send_text(json.dumps({
                "type": "status_update",
                "id": payload["id"],
                "status": "sent"
            }))

    except WebSocketDisconnect:
        logger.info(f"🔴 Socket disconnected (uid={my_uid})")
    except Exception as e:
        logger.error(f"🔥 Error (uid={my_uid}): {e}")
    finally:
        if my_uid and active_connections.get(my_uid) is websocket:
            del active_connections[my_uid]
            logger.info(f"❌ UID {my_uid} unbound")
