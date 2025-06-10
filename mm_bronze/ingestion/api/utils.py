import orjson
import xmltodict
import logging
import traceback
from fastapi import Request, HTTPException, status

from mm_bronze.common.config import settings

logger = logging.getLogger(__name__)


PARSERS = {
    "json": lambda raw: orjson.loads(raw),
    "xml": lambda raw: xmltodict.parse(raw),
    "text": lambda raw: raw.decode(),
}


async def get_raw_body(request: Request) -> bytes:
    """Get raw request body with size validation for Kafka message encoding overhead."""
    # Calculate effective max size accounting for base64 encoding overhead (~33% increase)
    # and JSON envelope overhead. Use 75% of Kafka max message size as safe limit.
    effective_max_size = int(settings.kafka_max_message_size * 0.75)

    # Check Content-Length header first if available
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            size = int(content_length)
            if size > effective_max_size:
                size_mb = size / (1024 * 1024)
                limit_mb = effective_max_size / (1024 * 1024)
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail={
                        "error": "File too large",
                        "message": (f"File size {size_mb:.1f}MB exceeds maximum allowed size of {limit_mb:.1f}MB"),
                        "file_size_bytes": size,
                        "max_size_bytes": effective_max_size,
                        "recommendation": ("For files larger than 100MB, please use SFTP upload instead"),
                    },
                )
        except ValueError:
            # Invalid Content-Length header, will be caught by body size check below
            pass

    # Get the actual body
    body = await request.body()

    # Validate actual body size
    actual_size = len(body)
    if actual_size > effective_max_size:
        size_mb = actual_size / (1024 * 1024)
        limit_mb = effective_max_size / (1024 * 1024)
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail={
                "error": "File too large",
                "message": f"File size {size_mb:.1f}MB exceeds maximum allowed size of {limit_mb:.1f}MB",
                "file_size_bytes": actual_size,
                "max_size_bytes": effective_max_size,
                "recommendation": "For files larger than 100MB, please use SFTP upload instead",
            },
        )

    return body


async def validate_payload(fmt: str, raw: bytes):
    parser = PARSERS.get(fmt)
    if not parser:
        raise HTTPException(status_code=400, detail=f"Unknown format '{fmt}'")
    try:
        parser(raw)
    except Exception:
        logger.error("Parser error:\n%s", traceback.format_exc())
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Malformed payload")


def default(obj):
    if isinstance(obj, bytes):
        return str(bytes)
