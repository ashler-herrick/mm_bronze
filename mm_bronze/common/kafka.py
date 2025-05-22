from typing import Optional, Dict
import logging
import asyncio

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from mm_bronze.common.config import settings

logger = logging.getLogger(__name__)

# ————— Async Consumer ——————————————————————————————————————

_async_consumers: Dict[str, AIOKafkaConsumer] = {}


def get_async_consumer(group: str) -> AIOKafkaConsumer:
    """
    Returns the singleton AIOKafkaConsumer.
    Call init_async_consumer() on startup before using.
    """
    _async_consumer = _async_consumers.get(group)
    if not _async_consumer:
        raise RuntimeError(
            f"Async consumer not initialized for group {group}; call init_async_consumer(group)"
        )
    return _async_consumer


async def init_async_consumer(
    group: str,
    topic: Optional[str] = None,
    max_retries: int = 5,
    base_backoff: float = 1.0,
) -> None:
    """
    Create & start the singleton AIOKafkaConsumer, with retry on bootstrap failure.
    """
    if not topic:
        topic = settings.kafka_api_raw_topic

    if group in _async_consumers:
        return

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_servers.split(","),
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    for attempt in range(1, max_retries + 1):
        try:
            await consumer.start()
            _async_consumers[group] = consumer
            logger.info("Kafka consumer started on attempt %d", attempt)
            return
        except KafkaConnectionError as e:
            logger.warning(
                "Kafka consumer bootstrap failed (attempt %d/%d): %s",
                attempt,
                max_retries,
                e,
            )
            if attempt == max_retries:
                logger.error("Exceeded max retries for consumer; giving up.")
                raise
            backoff = base_backoff * attempt
            await asyncio.sleep(backoff)


async def close_async_consumer(group: str) -> None:
    """
    Stop & cleanup the singleton AIOKafkaConsumer.
    """
    _async_consumer = _async_consumers[group]
    if _async_consumer is not None:
        await _async_consumer.stop()
        _async_consumer = None


# ————— Async Producer ——————————————————————————————————————

_async_producer: Optional[AIOKafkaProducer] = None


def get_async_producer() -> AIOKafkaProducer:
    """
    Returns the singleton AIOKafkaProducer.
    Call init_async_producer() on startup before using.
    """
    global _async_producer
    if _async_producer is None:
        raise RuntimeError("Async producer not initialized; call init_async_producer()")
    return _async_producer


async def init_async_producer(
    max_retries: int = 5,
    base_backoff: float = 1.0,
) -> None:
    """
    Create & start the singleton AIOKafkaProducer, with retry on bootstrap failure.
    """
    global _async_producer
    if _async_producer is not None:
        return

    _async_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_servers.split(","),
        value_serializer=lambda v: v,  # expect bytes
    )
    logger.info(f"Kafka servers: {settings.kafka_servers.split(',')}")

    for attempt in range(1, max_retries + 1):
        try:
            await _async_producer.start()
            logger.info("Kafka producer started on attempt %d", attempt)
            return
        except KafkaConnectionError as e:
            logger.warning(
                "Kafka producer bootstrap failed (attempt %d/%d): %s",
                attempt,
                max_retries,
                e,
            )
            if attempt == max_retries:
                logger.error("Exceeded max retries for producer; giving up.")
                raise
            backoff = base_backoff * attempt
            await asyncio.sleep(backoff)


async def close_async_producer() -> None:
    """
    Stop & cleanup the singleton AIOKafkaProducer.
    """
    global _async_producer
    if _async_producer is not None:
        await _async_producer.stop()
        _async_producer = None


def _get_avail_consumer_groups():
    return _async_consumers.keys()
