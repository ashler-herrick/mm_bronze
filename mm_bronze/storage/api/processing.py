import logging
import base64
from typing import Dict

import orjson
from asyncpg import UniqueViolationError

from mm_bronze.common.db import get_pool
from mm_bronze.common.fs import AsyncFS
from mm_bronze.storage.utils import compute_fingerprint, log_ingestion, write_to_storage

logger = logging.getLogger(__name__)


async def process_message(
    raw_msg: bytes,
    fs: AsyncFS,
) -> None:
    """
    Process a raw Kafka message: decode, fingerprint, metadata store, and write payload to storage.

    Args:
        raw_msg: The raw message bytes from Kafka, containing a JSON envelope.
        fs: An initialized AsyncFS instance for writing raw payloads.
    """
    try:
        envelope = orjson.loads(raw_msg)
    except orjson.JSONDecodeError as e:
        logger.exception(f"Expected bytes, got {type(raw_msg)}: {e}.")
        raise e
    uid: str = envelope["uuid"]

    await log_ingestion(uid, "started", None)

    # Decode payload
    payload_bytes = base64.b64decode(envelope["body"])

    # Compute fingerprint
    fingerprint = compute_fingerprint(payload_bytes)
    fp_hex = fingerprint.hex()

    # Build storage path
    path = build_path_by_fp(envelope, fp_hex)

    # Store metadata
    await store_metadata(event=envelope, fingerprint=fingerprint, path=path)

    # Write raw payload
    await write_to_storage(fs, path, payload_bytes, uid)


def build_path_by_fp(event: Dict[str, str], fp_hex: str, base_prefix: str = "bronze") -> str:
    """
    Construct a storage path using event metadata and fingerprint.

    Args:
        event: Dictionary containing keys 'content_type', 'format', 'subtype'.
        fingerprint:
        base_prefix: Top-level directory prefix (default 'bronze').

    Returns:
        A relative file path for storage (without leading slash).
    """
    short_fp = fp_hex[:16]
    file_ext = event["format"].lower()
    return "/".join(
        [
            base_prefix,
            event["content_type"],
            event["format"],
            event["subtype"],
            f"{short_fp}.{file_ext}",
        ]
    )


async def store_metadata(event: Dict[str, str], fingerprint: bytes, path: str) -> None:
    """
    Insert ingestion metadata into the raw_api_ingest table, logging duplicates.

    Args:
        event: Envelope containing 'uuid', 'format', 'content_type', 'subtype', and 'version'.
        fingerprint: Hex string of the payload fingerprint.
        path: Storage path where the payload will be written.
    """
    uid = event.get("uuid")
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                # Create source metadata for API ingestion
                source_metadata = {
                    "ingestion_method": "api",
                    "endpoint": (
                        f"/ingest/{event['format']}/{event['content_type']}/{event['version']}/{event['subtype']}"
                    ),
                    "format": event["format"],
                    "content_type": event["content_type"],
                    "subtype": event["subtype"],
                    "version": event["version"],
                }

                await conn.execute(
                    """
                    INSERT INTO ingestion.raw_ingestion
                        (object_id, ingestion_source, format, content_type, subtype,
                         data_version, storage_path, fingerprint, source_metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    uid,
                    "api",
                    event["format"],
                    event["content_type"],
                    event["subtype"],
                    event["version"],
                    path,
                    fingerprint,
                    orjson.dumps(source_metadata).decode("utf-8"),
                )
                status = "ingested"
        except UniqueViolationError:
            status = "duplicate"
            logger.warning("Duplicate fingerprint for %s", uid)

        # Log status in separate transaction to avoid rollback issues
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO ingestion.ingestion_log(object_id, status) VALUES ($1, $2)",
                uid,
                status,
            )
