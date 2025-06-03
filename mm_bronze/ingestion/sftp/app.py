"""
Main SFTP server application entry point.
"""

import os
import socket
import threading
import asyncio
import logging

from mm_bronze.common.kafka import init_async_producer, close_async_producer
from mm_bronze.common.log_config import configure_logging

from mm_bronze.ingestion.sftp.user_manager import UserManager
from mm_bronze.ingestion.sftp.server import load_host_key
from mm_bronze.ingestion.sftp.client import handle_client
from mm_bronze.common.config import settings

# Configure logging
configure_logging()
logger = logging.getLogger(__name__)

# Global event loop for Kafka operations
_event_loop = None


def set_global_event_loop(loop):
    """Set the global event loop for Kafka operations."""
    global _event_loop
    _event_loop = loop


def get_global_event_loop():
    """Get the global event loop for Kafka operations."""
    return _event_loop


async def start_async_components():
    """Initialize and run async components (Kafka producer)."""
    # Initialize Kafka producer
    await init_async_producer()

    # Set global event loop for Kafka operations
    set_global_event_loop(asyncio.get_running_loop())

    # Keep running until interrupted
    try:
        stop_event = asyncio.Event()
        await stop_event.wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down async components...")
    finally:
        await close_async_producer()


def run_async_background():
    """Run async components in background thread."""
    asyncio.run(start_async_components())


def main():
    """Main SFTP server entry point."""
    # Get configuration from environment or defaults
    host = settings.sftp_host
    port = settings.sftp_port
    upload_root = settings.sftp_upload_root
    host_key_path = settings.sftp_host_key_path

    # Initialize user manager
    user_manager = UserManager()

    # Start async components in background thread
    async_thread = threading.Thread(target=run_async_background, daemon=True)
    async_thread.start()

    # Load or generate host key
    host_key = load_host_key(host_key_path)

    # Create uploads directory
    os.makedirs(upload_root, exist_ok=True)
    os.chdir(upload_root)  # Change to uploads directory
    logger.info(f"Working directory: {os.getcwd()}")

    # Setup socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
    except Exception as e:
        logger.error(f"Bind failed: {e}")
        return

    try:
        sock.listen(100)
        logger.info(f"Listening for connection on {host}:{port}...")

        while True:
            client, addr = sock.accept()
            logger.info(f"Got connection from {addr}")

            # Handle client in background thread
            client_thread = threading.Thread(
                target=handle_client,
                args=(client, host_key, user_manager, get_global_event_loop()),
                daemon=True,
            )
            client_thread.start()

    except Exception as e:
        logger.error(f"Listen/accept failed: {e}")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
