"""
SFTP file handle implementation with Kafka event publishing.
"""

import os
import time
import logging
import asyncio

import paramiko
from paramiko import SFTPHandle
import orjson

from mm_bronze.common.config import settings
from mm_bronze.common.kafka import get_async_producer

logger = logging.getLogger(__name__)


class ProductionSFTPHandle(SFTPHandle):
    """SFTP file handle that publishes upload events to Kafka."""

    def __init__(self, flags=0, username=None, event_loop=None):
        """Initializes a file handle with optional user and event loop context.

        Args:
            flags (int): SFTP open flags.
            username (str, optional): Username performing the file operation.
            event_loop (asyncio.AbstractEventLoop, optional): Event loop for asynchronous Kafka publishing.
        """
        super().__init__(flags)
        self.filename = None
        self.readfile = None
        self.writefile = None
        self.username = username
        self.event_loop = event_loop
        self.bytes_written = 0  # Track total bytes written for size validation

    def close(self) -> int:
        """Closes the file handle and publishes an upload event if it was a write operation.

        Returns:
            int: SFTP_OK on success.
        """
        if self.readfile is not None:
            self.readfile.close()
            self.readfile = None

        if self.writefile is not None:
            self.writefile.close()
            if self.filename:
                logger.info(f"File upload completed by {self.username}: {self.filename}")
                # Publish Kafka event for upload completion
                if self.event_loop:
                    asyncio.run_coroutine_threadsafe(self._publish_upload_event(), self.event_loop)
            self.writefile = None
        return paramiko.SFTP_OK

    async def _publish_upload_event(self):
        """Publishes an upload completion event to the Kafka topic configured in settings.

        The payload includes path, timestamp, file size, event type, username, and source.
        """
        try:
            producer = get_async_producer()

            # Build event payload with user information
            payload = {
                "path": self.filename,
                "timestamp": time.time(),
                "size": os.path.getsize(self.filename) if os.path.exists(self.filename) else 0,
                "event_type": "sftp_upload_complete",
                "username": self.username,
                "source": "sftp_server",
            }

            topic = settings.kafka_bronze_sftp_topic
            await producer.send_and_wait(topic, orjson.dumps(payload))
            logger.info(f"Published upload event to {topic} for {self.filename} by user {self.username}")

        except Exception as e:
            logger.error(f"Failed to publish upload event for {self.filename}: {e}")

    def read(self, offset: int, length: int) -> bytes | int:
        """Reads data from the file starting at the given offset.

        Args:
            offset (int): Byte offset to start reading from.
            length (int): Number of bytes to read.

        Returns:
            bytes or int: The read data, or SFTP_FAILURE on error.
        """
        if self.readfile is None:
            return paramiko.SFTP_FAILURE
        try:
            self.readfile.seek(offset)
            return self.readfile.read(length)
        except OSError:
            return paramiko.SFTP_FAILURE

    def write(self, offset: int, data: bytes) -> int:
        """Writes data to the file at the given offset with size validation.

        Args:
            offset (int): Byte offset to start writing at.
            data (bytes): Data to write.

        Returns:
            int: SFTP_OK on success, or SFTP_FAILURE on error.
        """
        if self.writefile is None:
            return paramiko.SFTP_FAILURE

        # Check if this write would exceed the SFTP file size limit
        try:
            current_size = self.writefile.tell()
            projected_size = max(current_size, offset + len(data))

            if projected_size > settings.sftp_max_file_size:
                size_mb = projected_size / (1024 * 1024)
                limit_mb = settings.sftp_max_file_size / (1024 * 1024)
                logger.warning(
                    f"SFTP upload size limit exceeded by {self.username}: "
                    f"{size_mb:.1f}MB > {limit_mb:.1f}MB for file {self.filename}"
                )
                return paramiko.SFTP_FAILURE

            self.writefile.seek(offset)
            self.writefile.write(data)
            self.writefile.flush()

            # Update bytes written counter
            self.bytes_written = max(self.bytes_written, offset + len(data))

        except OSError as e:
            logger.error(f"SFTP write error for {self.filename}: {e}")
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def stat(self) -> paramiko.SFTPAttributes:
        """Returns file statistics using paramiko's SFTPAttributes.

        Returns:
            paramiko.SFTPAttributes: File metadata, or empty attributes on error.
        """
        try:
            if self.writefile:
                return paramiko.SFTPAttributes.from_stat(os.fstat(self.writefile.fileno()))
            elif self.readfile:
                return paramiko.SFTPAttributes.from_stat(os.fstat(self.readfile.fileno()))
            else:
                return paramiko.SFTPAttributes()
        except OSError:
            return paramiko.SFTPAttributes()
