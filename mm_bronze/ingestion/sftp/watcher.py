import os
import time
import asyncio
import logging

import orjson
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from mm_bronze.common.config import settings
from mm_bronze.common.kafka import (
    init_async_producer,
    get_async_producer,
    close_async_producer,
)
from mm_bronze.common.log_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

WATCH_ROOT = settings.watch_root
TOPIC = settings.kafka_bronze_sftp_topic


class NewFileHandler(FileSystemEventHandler):
    def __init__(self, loop):
        super().__init__()
        self.loop = loop

    def on_created(self, event):
        # skip directories
        if event.is_directory:
            return

        # build payload
        payload = {
            "path": event.src_path,
            "timestamp": time.time(),
            "size": os.path.getsize(event.src_path),
        }

        logger.info("Detected new file, scheduling publish: %s", payload["path"])
        # schedule an async send
        self.loop.call_soon_threadsafe(
            lambda: self.loop.create_task(self._send(payload))
        )

    async def _send(self, payload: dict):
        producer = get_async_producer()
        # aiokafka Producer expects bytes
        await producer.send_and_wait(TOPIC, orjson.dumps(payload))
        logger.info("Published event to %s for %s", TOPIC, payload["path"])


async def main():
    # 1) init Kafka producer
    await init_async_producer()

    # 2) start watchdog observer
    loop = asyncio.get_running_loop()
    handler = NewFileHandler(loop)
    observer = Observer()
    observer.schedule(handler, WATCH_ROOT, recursive=True)
    observer.start()
    logger.info("Watching %s recursively – publishing to %s", WATCH_ROOT, TOPIC)

    # 3) run until interrupted
    stop = asyncio.Event()
    try:
        await stop.wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down watcher…")
    finally:
        observer.stop()
        observer.join()
        await close_async_producer()


if __name__ == "__main__":
    asyncio.run(main())
