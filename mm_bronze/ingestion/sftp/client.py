"""
Client connection handling for SFTP server.
"""

import time
import logging
import paramiko
import asyncio
from paramiko import SFTPServer

from .user_manager import UserManager
from .server import ProductionServer, ProductionSFTPServer

logger = logging.getLogger(__name__)


def handle_client(
    client,
    host_key: paramiko.PKey,
    user_manager: UserManager,
    event_loop: asyncio.AbstractEventLoop = None,
):
    """Handles a single SFTP client connection using Paramiko's SSH transport.

    This function sets up the SSH server transport, authenticates the client using
    the provided `UserManager`, and configures the SFTP subsystem with permission
    controls and event publishing.

    Args:
        client: A socket-like object representing the client connection.
        host_key: The SSH server host key (paramiko.PKey).
        user_manager (UserManager): Manages user credentials and permissions.
        event_loop (asyncio.AbstractEventLoop, optional): Event loop for async operations like Kafka publishing.

    Returns:
        None
    """

    try:
        transport = paramiko.Transport(client)
        transport.add_server_key(host_key)

        # Create server interface with user manager
        server = ProductionServer(user_manager)

        # Setup SFTP subsystem - create a factory function that captures our dependencies
        def sftp_factory(channel, name, server, *largs, **kwargs):
            return ProductionSFTPServer(
                server,
                user_manager=user_manager,
                event_loop=event_loop,
                *largs,
                **kwargs,
            )

        transport.set_subsystem_handler("sftp", SFTPServer, sftp_si=sftp_factory)
        transport.start_server(server=server)

        # Wait for auth
        channel = transport.accept(20)
        if channel is None:
            logger.warning("No channel established")
            return

        logger.info(f"Client authenticated as user: {server.authenticated_user}")

        # Keep connection alive
        try:
            while transport.is_active():
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    except Exception as e:
        logger.error(f"Error handling client: {e}")
        import traceback

        traceback.print_exc()
    finally:
        try:
            transport.close()
        except:
            pass
        try:
            client.close()
        except:
            pass
