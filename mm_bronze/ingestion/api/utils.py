import orjson
import xmltodict
import logging
import traceback
from fastapi import Request, HTTPException, status

logger = logging.getLogger()


PARSERS = {
    "json":  lambda raw: orjson.loads(raw),
    "xml":   lambda raw: xmltodict.parse(raw),
    "text":  lambda raw: raw.decode(),
}

async def get_raw_body(request: Request) -> bytes:
    return await request.body()


async def validate_payload(
    fmt: str, 
    raw: bytes
):
    parser = PARSERS.get(fmt)
    if not parser:
        raise HTTPException(
            status_code=400, detail=f"Unknown format '{fmt}'"
        )
    try:
        parser(raw)
    except Exception:
        logger.error("Parser error:\n%s", traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Malformed payload"
        )
    

def default(obj):
    if isinstance(obj, bytes):
        return str(bytes)