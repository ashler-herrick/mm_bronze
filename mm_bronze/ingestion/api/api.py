"""
Defines a generic ingestion endpoint:
- POST /ingest/{fmt}/{standard}/{version}/{subtype}
- Validates payload
- Computes UUID
- Transforms payload to raw b64 str
- Publishes a uniform event to Kafka
"""

import base64
from uuid import uuid4
import logging

import orjson
from fastapi import APIRouter, Depends, status

from mm_bronze.ingestion.api.utils import get_raw_body, validate_payload
from mm_bronze.common.kafka import get_async_producer
from mm_bronze.common.config import settings

router = APIRouter(prefix="/ingest", tags=["ingest"])
logger = logging.getLogger(__name__)


# Examples:
# /ingest/json/fhir/r6/patient
# /ingest/xml/fhir/r6/patient
# /ingest/text/hl7/v2/lab
@router.post(
    "/{fmt}/{content_type}/{version}/{subtype}",
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_entity(
    fmt: str,
    content_type: str,
    version: str,
    subtype: str,
    raw: bytes = Depends(get_raw_body),
    producer=Depends(get_async_producer),
):
    logger.info(f"Received ingestion request at ingestion/{fmt}/{version}/{subtype}")

    # validate the payload valid json/xml/etc.
    await validate_payload(fmt, raw)

    # Compute a uid for tracking
    uid = str(uuid4())

    # Encode a base64 string that orjson can serialize
    # TODO: Check that b64 encoded FHIR entries dont get messed up
    raw_b64 = base64.b64encode(raw).decode("ascii")

    # Build the uniform event envelope
    event = {
        "uuid": uid,
        "format": fmt,  # e.g. json, xml, text
        "content_type": content_type,  # e.g. fhir, hl7
        "version": version,  # e.g. r6, v3
        "subtype": subtype,  # e.g. patient, bundle, lab
        "body": raw_b64,  # base64 encoded string of the incoming payload
    }

    # Publish to Kafka
    topic = settings.kafka_bronze_api_topic
    await producer.send_and_wait(topic, orjson.dumps(event))
    # Note: flush behavior controlled by producer config
    logger.info("Event queued for Kafka")

    return {"status": "queued", "uuid": uid}
