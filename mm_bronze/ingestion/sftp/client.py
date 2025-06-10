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
    """Handles a single SFTP client connection using Paramiko's SSH transport."""

    try:
        logger.info("Creating transport for new client")
        transport = paramiko.Transport(client)
        transport.add_server_key(host_key)

        # Create server interface with user manager
        logger.info("Creating ProductionServer instance")
        server = ProductionServer(user_manager)

        def sftp_factory(ssh_server, *largs, **kwargs):
            """Factory function for creating SFTP server instances.

            Args:
                ssh_server: The SSH server instance (ProductionServer)
                *largs, **kwargs: Additional arguments
            """

            try:
                sftp_server = ProductionSFTPServer(
                    ssh_server,
                    user_manager=user_manager,
                    event_loop=event_loop,
                    *largs,
                    **kwargs,
                )
                logger.info(f"Successfully created ProductionSFTPServer: {sftp_server}")
                return sftp_server
            except Exception as e:
                logger.error(f"Failed to create ProductionSFTPServer: {e}")
                import traceback

                traceback.print_exc()
                raise

        # Register the SFTP subsystem
        logger.info("Registering SFTP subsystem handler")
        transport.set_subsystem_handler("sftp", SFTPServer, sftp_si=sftp_factory)

        # Start the server
        logger.info("Starting SSH server")
        transport.start_server(server=server)

        # Wait for auth
        logger.info("Waiting for channel establishment...")
        channel = transport.accept(30)
        if channel is None:
            logger.warning("No channel established within timeout")
            return

        logger.info(f"Channel established! Authenticated user: {server.authenticated_user}")

        # Keep connection alive
        try:
            while transport.is_active():
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")

    except Exception as e:
        logger.error(f"Error handling client: {e}")
        import traceback

        traceback.print_exc()
    finally:
        logger.info("Cleaning up client connection")
        transport.close()
        client.close()
