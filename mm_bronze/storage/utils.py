"""
Shared storage processing utilities for both API and SFTP ingestion.

This module contains common functions used by both storage processors to avoid code duplication.
"""
import logging
import hashlib
from typing import Optional

from mm_bronze.common.db import get_pool
from mm_bronze.common.fs import AsyncFS

logger = logging.getLogger(__name__)


def compute_fingerprint(data: bytes) -> bytes:
    """
    Compute the SHA-256 hash of the given data.

    Args:
        data: Raw bytes to hash.

    Returns:
        The hash digest as raw bytes.
    """
    hasher = hashlib.sha256()
    hasher.update(data)
    return hasher.digest()


async def log_ingestion(object_id: str, status: str, message: Optional[str]) -> None:
    """
    Log ingestion status and optional message to ingestion_log table.

    Args:
        object_id: The unique identifier of the ingested object.
        status: One of 'started', 'ingested', 'complete', 'failed', etc.
        message: Optional error or informational message.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ingestion.ingestion_log(object_id, status, message)
            VALUES ($1, $2, $3)
            """,
            object_id,
            status,
            message,
        )


async def write_to_storage(
    fs: AsyncFS, path: str, data: bytes, uid: str, log_success_message: Optional[str] = None
) -> bool:
    """
    Write raw payload bytes to storage and log completion or failure.

    Args:
        fs: AsyncFS filesystem client.
        path: Relative storage path.
        data: Raw bytes to write.
        uid: The unique identifier for logging purposes.
        log_success_message: Optional custom message to log on success.

    Returns:
        bool: True if storage write was successful, False otherwise.
    """
    try:
        await fs.write_bytes(str(path), data)
        await log_ingestion(uid, "complete", None)
        
        # Log success with custom message if provided, otherwise use default
        if log_success_message:
            logger.info(log_success_message)
        else:
            logger.info(f"Successfully stored data to {path}")
        
        return True
    except Exception as e:
        logger.exception("Failed to write payload for %s", uid)
        await log_ingestion(uid, "failed", str(e))
        return False