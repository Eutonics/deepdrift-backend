"""
Firebase Cloud Messaging push-уведомления.
"""
import asyncio
import json
import logging
from typing import Optional

import firebase_admin
from firebase_admin import credentials, messaging

from config import FB_JSON

logger = logging.getLogger("DDChatRelay")


class PushService:
    """Отправка push-уведомлений через Firebase."""

    def __init__(self):
        self._initialized = False
        self._init_firebase()

    def _init_firebase(self):
        try:
            if FB_JSON:
                fb_dict = json.loads(FB_JSON)
                cred = credentials.Certificate(fb_dict)
                firebase_admin.initialize_app(cred)
                self._initialized = True
                logger.info("✅ Firebase Admin SDK initialized")
            else:
                logger.warning("⚠️ FIREBASE_SERVICE_ACCOUNT_JSON is missing! Push disabled.")
        except Exception as e:
            logger.error(f"❌ Firebase Error: {e}")

    @property
    def available(self) -> bool:
        return self._initialized and bool(firebase_admin._apps)

    async def send_push(
        self,
        redis_client,
        target_uid: str,
        from_uid: str,
        message_type: str = "new_message",
        group_id: Optional[str] = None,
    ):
        """Отправляет push-уведомление через FCM."""
        if not self.available or not redis_client:
            return
        try:
            token = await redis_client.get(f"fcm_token:{target_uid}")
            if not token:
                return

            sender_profile = await redis_client.hgetall(f"profile:{from_uid}")
            sender_name = sender_profile.get("nickname", from_uid) if sender_profile else from_uid

            # FIX: target_uid всегда передаётся — клиент использует его чтобы
            # запросить офлайн-очередь нужного чата при открытии по пушу.
            # Для групп target_uid = group_id, для личных = from_uid.
            chat_uid = group_id if group_id else from_uid

            data_payload = {
                "from_uid":     from_uid,
                "target_uid":   chat_uid,
                "sender_name":  sender_name,
                "type":         message_type,
                "click_action": "FLUTTER_NOTIFICATION_CLICK",
            }

            msg = messaging.Message(
                data=data_payload,
                notification=messaging.Notification(
                    title="DDChat",
                    body="Новое зашифрованное сообщение",
                ),
                token=token,
                android=messaging.AndroidConfig(
                    priority="high",
                    notification=messaging.AndroidNotification(
                        channel_id="chat_messages",
                        # AndroidNotificationPriority не поддерживается в firebase-admin 6.x,
                        # приоритет уже задан на уровне AndroidConfig через priority="high"
                        default_vibrate_timings=True,
                        default_sound=True,
                    ),
                ),
                apns=messaging.APNSConfig(
                    headers={"apns-priority": "10"},
                    payload=messaging.APNSPayload(
                        aps=messaging.Aps(
                            badge=1,
                            sound="default",
                            content_available=True,
                        )
                    ),
                ),
            )
            await asyncio.get_event_loop().run_in_executor(None, messaging.send, msg)
            logger.info(f"📲 Push sent to {target_uid} ({message_type}) from {from_uid}")
        except messaging.UnregisteredError:
            # Токен протух — удаляем из Redis, клиент перерегистрирует при следующем подключении
            logger.warning(f"⚠️ FCM token expired for {target_uid}, removing")
            try:
                await redis_client.delete(f"fcm_token:{target_uid}")
            except Exception:
                pass
        except Exception as e:
            logger.error(f"❌ Push error: {e}")
