"""
Бэкенд хранения файлов: Cloudflare R2 или локальный диск.
"""
import asyncio
import io
import logging
import os
import uuid
from typing import Optional, Tuple

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config import R2_ENDPOINT, R2_KEY_ID, R2_SECRET, R2_BUCKET, UPLOAD_DIR, MAX_UPLOAD_SIZE

logger = logging.getLogger("DDChatRelay")

os.makedirs(UPLOAD_DIR, exist_ok=True)


class StorageBackend:
    """Абстракция над хранилищем файлов."""

    def __init__(self):
        self._r2 = self._init_r2()
        if self._r2:
            logger.info(f"✅ R2 storage configured: bucket={R2_BUCKET}")
        else:
            logger.warning("⚠️ R2 not configured — using local disk (ephemeral on Render!)")

    @property
    def storage_type(self) -> str:
        return f"r2:{R2_BUCKET}" if self._r2 else "local_disk (ephemeral!)"

    @staticmethod
    def _init_r2():
        if not all([R2_ENDPOINT, R2_KEY_ID, R2_SECRET]):
            return None
        return boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_KEY_ID,
            aws_secret_access_key=R2_SECRET,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
            region_name="auto",
        )

    async def upload(self, file_data: bytes, filename: str, content_type: str) -> str:
        """Загружает файл, возвращает file_id."""
        safe_name = os.path.basename(filename or "file")
        file_id = f"{uuid.uuid4().hex}_{safe_name}"

        if self._r2:
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._r2.put_object(
                    Bucket=R2_BUCKET,
                    Key=file_id,
                    Body=file_data,
                    ContentType=content_type or "application/octet-stream",
                ),
            )
            logger.info(f"📦 Uploaded to R2: {file_id} ({len(file_data)} bytes)")
        else:
            file_path = os.path.join(UPLOAD_DIR, file_id)
            with open(file_path, "wb") as f:
                f.write(file_data)
            logger.info(f"💾 Saved locally: {file_id} ({len(file_data)} bytes)")

        return file_id

    async def download(self, file_id: str) -> Optional[Tuple[bytes, str]]:
        """Скачивает файл, возвращает (bytes, content_type) или None."""
        safe_file_id = os.path.basename(file_id)

        if self._r2:
            try:
                obj = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._r2.get_object(Bucket=R2_BUCKET, Key=safe_file_id),
                )
                body = obj["Body"].read()
                content_type = obj.get("ContentType", "application/octet-stream")
                return body, content_type
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("NoSuchKey", "404"):
                    logger.warning(f"⚠️ R2 Download not found: {safe_file_id}")
                    return None
                logger.error(f"❌ R2 Download error: {e}")
                raise
        else:
            file_path = os.path.join(UPLOAD_DIR, safe_file_id)
            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    data = f.read()
                return data, "application/octet-stream"
            logger.warning(f"⚠️ Local Download not found: {safe_file_id}")
            return None
