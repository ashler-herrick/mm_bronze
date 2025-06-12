import logging
import uuid
import os
from pathlib import Path

import orjson
from asyncpg import UniqueViolationError

from mm_bronze.common.db import get_pool
from mm_bronze.common.fs import AsyncFS
from mm_bronze.storage.utils import compute_fingerprint, log_ingestion, write_to_storage

logger = logging.getLogger(__name__)


async def process_sftp_message(
    raw_msg: bytes,
    fs: AsyncFS,
) -> None:
    """
    Process a raw SFTP Kafka message: read file, fingerprint, metadata store, and move to storage.

    Args:
        raw_msg: The raw message bytes from Kafka, containing a JSON envelope with file info.
        fs: An initialized AsyncFS instance for writing raw payloads.
    """
    try:
        envelope = orjson.loads(raw_msg)
    except orjson.JSONDecodeError as e:
        logger.exception(f"Expected bytes, got {type(raw_msg)}: {e}.")
        raise e

    # Generate UUID for this file if not present
    file_uuid = str(uuid.uuid4())

    await log_ingestion(file_uuid, "started", None)

    # Extract file information from SFTP event
    file_path = envelope["path"]
    username = envelope.get("username", "unknown")
    size = envelope.get("size", 0)

    logger.info(f"Processing SFTP file: {file_path} (size: {size}) uploaded by {username}")

    try:
        # Read file contents
        payload_bytes = await read_uploaded_file(file_path)

        # Compute fingerprint
        fingerprint = compute_fingerprint(payload_bytes)
        fp_hex = fingerprint.hex()

        # Build storage path using file information
        path = build_path_for_sftp_file(file_path, fp_hex, username)

        # Store metadata
        await store_sftp_metadata(
            file_uuid=file_uuid,
            original_path=file_path,
            username=username,
            size=size,
            fingerprint=fingerprint,
            storage_path=path,
        )

        # Write to storage with custom success message
        storage_success = await write_to_storage(
            fs, path, payload_bytes, file_uuid, log_success_message=f"Successfully stored SFTP file to {path}"
        )

        # Only clean up original file if storage was successful
        if storage_success:
            await cleanup_uploaded_file(file_path, file_uuid)
        else:
            logger.warning(f"Storage failed for {file_path}, preserving original file")
            await log_ingestion(file_uuid, "storage_failed", f"Preserving original file: {file_path}")

    except Exception as e:
        logger.exception(f"Failed to process SFTP file {file_path}")
        await log_ingestion(file_uuid, "failed", str(e))
        raise


async def read_uploaded_file(file_path: str) -> bytes:
    """
    Read the uploaded file from the SFTP server filesystem.

    Args:
        file_path: Path to the uploaded file

    Returns:
        File contents as bytes
    """
    import os

    try:
        if not os.path.exists(file_path):
            logger.warning(f"File {file_path} does not exist, may have been cleaned up already")
            raise FileNotFoundError(f"File {file_path} not found")

        with open(file_path, "rb") as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {e}")
        raise


def build_path_for_sftp_file(original_path: str, fp_hex: str, username: str, base_prefix: str = "bronze") -> str:
    """
    Construct a storage path for SFTP uploaded files.

    Args:
        original_path: Original file path from SFTP upload
        fp_hex: Hex string of the file fingerprint
        username: Username who uploaded the file
        base_prefix: Top-level directory prefix (default 'bronze').

    Returns:
        A relative file path for storage (without leading slash).
    """
    # Extract filename and extension from original path
    path_obj = Path(original_path)
    file_ext = path_obj.suffix.lstrip(".") or "bin"

    short_fp = fp_hex[:16]

    return "/".join(
        [
            base_prefix,
            "sftp",
            username,
            f"{short_fp}.{file_ext}",
        ]
    )


async def store_sftp_metadata(
    file_uuid: str,
    original_path: str,
    username: str,
    size: int,
    fingerprint: bytes,
    storage_path: str,
) -> None:
    """
    Insert SFTP ingestion metadata into the database.

    Args:
        file_uuid: Generated UUID for this file
        original_path: Original file path from SFTP upload
        username: Username who uploaded the file
        size: File size in bytes
        fingerprint: SHA-256 fingerprint of file contents
        storage_path: Path where file will be stored
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Determine file format and content type
        path_obj = Path(original_path)
        file_ext = path_obj.suffix.lstrip(".").lower()

        # Map file extensions to content types
        content_type_map = {
            "json": "json",
            "xml": "xml",
            "txt": "text",
            "csv": "text",
            "pdf": "binary",
            "dcm": "binary",
            "zip": "binary",
        }
        content_type = content_type_map.get(file_ext, "binary")

        # Use filename (without extension) as subtype for categorization
        subtype = path_obj.stem or "document"

        # Create source metadata with SFTP-specific information
        source_metadata = {
            "username": username,
            "original_path": original_path,
            "original_filename": path_obj.name,
            "file_size": size,
            "upload_method": "sftp",
        }

        try:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO ingestion.raw_ingestion
                        (object_id, ingestion_source, format, content_type, subtype,
                         data_version, storage_path, fingerprint, source_metadata)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    file_uuid,
                    "sftp",
                    file_ext or "unknown",  # format is file extension
                    content_type,  # content_type is data format
                    subtype,  # subtype is filename/category
                    "1.0",  # data_version
                    storage_path,
                    fingerprint,
                    orjson.dumps(source_metadata).decode("utf-8"),  # source_metadata as JSONB string
                )
                status = "ingested"
        except UniqueViolationError:
            status = "duplicate"
            logger.warning("Duplicate fingerprint for SFTP file %s", file_uuid)

        # Log status in separate transaction to avoid rollback issues
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO ingestion.ingestion_log(object_id, status, message) 
                VALUES ($1, $2, $3)
                """,
                file_uuid,
                status,
                f"SFTP upload: {original_path} by {username} ({size} bytes)",
            )


async def cleanup_uploaded_file(file_path: str, file_uuid: str) -> None:
    """
    Clean up the original uploaded file after successful processing.

    Args:
        file_path: Path to the uploaded file to clean up
        file_uuid: UUID for logging purposes
    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up uploaded file: {file_path}")
            await log_ingestion(file_uuid, "cleaned_up", f"Removed original file: {file_path}")
    except Exception as e:
        logger.warning(f"Failed to clean up file {file_path}: {e}")
        await log_ingestion(file_uuid, "cleanup_failed", str(e))
