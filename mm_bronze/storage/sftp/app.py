import logging
import asyncio

from mm_bronze.common.fs import AsyncFS
from mm_bronze.common.kafka import (
    get_async_consumer,
    init_async_consumer,
    close_async_consumer,
)
from mm_bronze.common.config import settings
from mm_bronze.common.log_config import configure_logging
from mm_bronze.storage.sftp.processing import process_sftp_message

configure_logging()
logger = logging.getLogger(__name__)


async def main():
    # config
    topic = settings.kafka_bronze_sftp_topic
    group = settings.kafka_bronze_sftp_group

    raw_storage_url = settings.raw_storage_url

    # Filesystem
    fs = AsyncFS(raw_storage_url)

    # Kafka consumer
    await init_async_consumer(group, topic)
    consumer = get_async_consumer(group)

    # Process all messages
    try:
        async for msg in consumer:
            raw = msg.value

            record_meta = {
                "topic": msg.topic,
                "partition": msg.partition,
                "offset": msg.offset,
                "timestamp": msg.timestamp,
                "headers": {k: v for k, v in msg.headers},
            }
            logger.info(f"Got SFTP message {record_meta}")
            try:
                await process_sftp_message(raw, fs)
                await consumer.commit()
            except Exception as e:
                logger.error(f"Failed to process SFTP message: {e}")
                # Continue processing other messages instead of crashing
                await consumer.commit()  # Still commit to avoid reprocessing
    finally:
        await close_async_consumer(group)


if __name__ == "__main__":
    asyncio.run(main())
