"""
Production-ready SFTP server with SSH key authentication and proper user management.
Enhanced for Milestone 2 preparation.
"""

import os
import socket
import threading
import paramiko
from paramiko import SFTPServerInterface, SFTPServer, SFTPAttributes, SFTPHandle
import logging
import asyncio
import time
import orjson
import base64
from pathlib import Path
from typing import Dict, Optional, Set

# Import our Kafka modules
from mm_bronze.common.config import settings
from mm_bronze.common.kafka import init_async_producer, get_async_producer, close_async_producer
from mm_bronze.common.log_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

# Global event loop for Kafka operations
_event_loop = None

def set_global_event_loop(loop):
    global _event_loop
    _event_loop = loop

def get_global_event_loop():
    return _event_loop


class UserManager:
    """Manages SFTP users with both password and SSH key authentication."""
    
    def __init__(self, settings_instance=None):
        self.users = {}  # username -> {'password': str, 'keys': [PKey], 'permissions': set}
        self.settings = settings_instance or settings
        self._load_users_from_settings()
        self._load_ssh_keys()
    
    def _load_users_from_settings(self):
        """Load users from centralized settings."""
        try:
            parsed_users = self.settings.get_sftp_users()
            
            for username, user_data in parsed_users.items():
                self.users[username] = {
                    'password': user_data['password'],
                    'keys': [],
                    'permissions': user_data['permissions']
                }
                logger.info(f"Loaded user {username} with permissions: {user_data['permissions']}")
                
        except Exception as e:
            logger.error(f"Failed to load users from settings: {e}")
            # Fallback to default user
            self.users['alice'] = {
                'password': 'secret',
                'keys': [],
                'permissions': {'read', 'write'}
            }
            logger.warning("Using fallback default user 'alice'")
    
    def _load_ssh_keys(self):
        """Load SSH public keys for users from the keys directory."""
        keys_dir = Path(self.settings.sftp_keys_dir)
        
        if not keys_dir.exists():
            logger.info(f"SSH keys directory {keys_dir} doesn't exist, skipping key loading")
            return
        
        for username in self.users:
            user_key_file = keys_dir / f"{username}.pub"
            if user_key_file.exists():
                try:
                    with open(user_key_file, 'r') as f:
                        key_data = f.read().strip()
                    
                    # Parse the public key
                    parts = key_data.split()
                    if len(parts) >= 2:
                        key_type = parts[0]
                        key_blob = base64.b64decode(parts[1])
                        
                        if key_type.startswith('ssh-rsa'):
                            key = paramiko.RSAKey(data=key_blob)
                        elif key_type.startswith('ssh-ed25519'):
                            key = paramiko.Ed25519Key(data=key_blob)
                        elif key_type.startswith('ecdsa-sha2'):
                            key = paramiko.ECDSAKey(data=key_blob)
                        else:
                            logger.warning(f"Unsupported key type {key_type} for user {username}")
                            continue
                        
                        self.users[username]['keys'].append(key)
                        logger.info(f"Loaded SSH key for user {username} (type: {key_type})")
                        
                except Exception as e:
                    logger.error(f"Failed to load SSH key for user {username}: {e}")
    
    def authenticate_password(self, username: str, password: str) -> bool:
        """Authenticate user with password."""
        user = self.users.get(username)
        if user and user['password'] == password:
            logger.info(f"Password authentication successful for {username}")
            return True
        logger.warning(f"Password authentication failed for {username}")
        return False
    
    def authenticate_key(self, username: str, key: paramiko.PKey) -> bool:
        """Authenticate user with SSH key."""
        user = self.users.get(username)
        if not user:
            logger.warning(f"Key authentication failed: unknown user {username}")
            return False
        
        for user_key in user['keys']:
            if key.get_name() == user_key.get_name() and key.asbytes() == user_key.asbytes():
                logger.info(f"SSH key authentication successful for {username}")
                return True
        
        logger.warning(f"SSH key authentication failed for {username}")
        return False
    
    def get_user_permissions(self, username: str) -> Set[str]:
        """Get permissions for a user."""
        user = self.users.get(username)
        return user['permissions'] if user else set()
    
    def user_exists(self, username: str) -> bool:
        """Check if user exists."""
        return username in self.users


class ProductionSFTPHandle(SFTPHandle):
    def __init__(self, flags=0, username=None):
        super().__init__(flags)
        self.filename = None
        self.readfile = None
        self.writefile = None
        self.username = username

    def close(self):
        if self.readfile is not None:
            self.readfile.close()
            self.readfile = None
        if self.writefile is not None:
            self.writefile.close()
            if self.filename:
                logger.info(f"File upload completed by {self.username}: {self.filename}")
                # Publish Kafka event for upload completion
                event_loop = get_global_event_loop()
                if event_loop:
                    asyncio.run_coroutine_threadsafe(
                        self._publish_upload_event(), 
                        event_loop
                    )
            self.writefile = None
        return paramiko.SFTP_OK

    async def _publish_upload_event(self):
        """Publish upload event to Kafka with enhanced metadata."""
        try:
            producer = get_async_producer()
            
            # Build event payload with user information
            payload = {
                "path": self.filename,
                "timestamp": time.time(),
                "size": os.path.getsize(self.filename) if os.path.exists(self.filename) else 0,
                "event_type": "sftp_upload_complete",
                "username": self.username,
                "source": "sftp_server"
            }
            
            topic = settings.kafka_bronze_sftp_topic
            await producer.send_and_wait(topic, orjson.dumps(payload))
            logger.info(f"Published upload event to {topic} for {self.filename} by user {self.username}")
            
        except Exception as e:
            logger.error(f"Failed to publish upload event for {self.filename}: {e}")

    def read(self, offset, length):
        if self.readfile is None:
            return paramiko.SFTP_FAILURE
        try:
            self.readfile.seek(offset)
            return self.readfile.read(length)
        except OSError:
            return paramiko.SFTP_FAILURE

    def write(self, offset, data):
        if self.writefile is None:
            return paramiko.SFTP_FAILURE
        try:
            self.writefile.seek(offset)
            self.writefile.write(data)
            self.writefile.flush()
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def stat(self):
        try:
            if self.writefile:
                return SFTPAttributes.from_stat(os.fstat(self.writefile.fileno()))
            elif self.readfile:
                return SFTPAttributes.from_stat(os.fstat(self.readfile.fileno()))
            else:
                return SFTPAttributes()
        except OSError:
            return SFTPAttributes()


class ProductionSFTPServer(SFTPServerInterface):
    def __init__(self, server, user_manager=None, *args, **kwargs):
        super().__init__(server, *args, **kwargs)
        self.upload_root = os.getcwd()
        self.user_manager = user_manager or kwargs.get('user_manager')
        self.current_user = getattr(server, 'authenticated_user', None)
        
        # Create user-specific directory (federated access)
        if self.current_user:
            self.user_root = os.path.join(self.upload_root, self.current_user)
            os.makedirs(self.user_root, exist_ok=True)
            logger.info(f"SFTP server root for user {self.current_user}: {self.user_root}")
        else:
            self.user_root = self.upload_root
            logger.info(f"SFTP server root: {self.upload_root}")

    def _realpath(self, path):
        """Resolve path with security checks and federated access."""
        # Remove leading slash and resolve relative to user's directory
        clean_path = path.lstrip('/')
        full_path = os.path.abspath(os.path.join(self.user_root, clean_path))
        
        # Security check: ensure path is within the user's directory
        try:
            Path(full_path).resolve().relative_to(Path(self.user_root).resolve())
        except ValueError:
            logger.warning(f"Path traversal attempt blocked: {path} by user {self.current_user}")
            raise paramiko.SFTPError(paramiko.SFTP_PERMISSION_DENIED, "Access denied")
        
        return full_path

    def _check_permission(self, operation: str) -> bool:
        """Check if current user has permission for operation."""
        if not self.current_user or not self.user_manager:
            return False
        
        permissions = self.user_manager.get_user_permissions(self.current_user)
        
        # Map operations to required permissions
        permission_map = {
            'read': 'read',
            'write': 'write', 
            'delete': 'delete',
            'mkdir': 'write',
            'rmdir': 'delete',
            'rename': 'write'
        }
        
        required_permission = permission_map.get(operation, 'read')
        has_permission = required_permission in permissions
        
        if not has_permission:
            logger.warning(f"Permission denied: user {self.current_user} attempted {operation} "
                         f"but only has permissions: {permissions}")
        
        return has_permission

    def list_folder(self, path):
        if not self._check_permission('read'):
            return paramiko.SFTP_PERMISSION_DENIED
            
        path = self._realpath(path)
        try:
            out = []
            flist = os.listdir(path)
            for fname in flist:
                attr = SFTPAttributes.from_stat(os.stat(os.path.join(path, fname)))
                attr.filename = fname
                out.append(attr)
            return out
        except OSError:
            return paramiko.SFTP_NO_SUCH_FILE

    def stat(self, path):
        if not self._check_permission('read'):
            return paramiko.SFTP_PERMISSION_DENIED
            
        path = self._realpath(path)
        try:
            return SFTPAttributes.from_stat(os.stat(path))
        except OSError:
            return paramiko.SFTP_NO_SUCH_FILE

    def lstat(self, path):
        if not self._check_permission('read'):
            return paramiko.SFTP_PERMISSION_DENIED
            
        path = self._realpath(path)
        try:
            return SFTPAttributes.from_stat(os.lstat(path))
        except OSError:
            return paramiko.SFTP_NO_SUCH_FILE

    def open(self, path, flags, attr):
        # Check permissions based on file operation
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT):
            if not self._check_permission('write'):
                return paramiko.SFTP_PERMISSION_DENIED
        else:
            if not self._check_permission('read'):
                return paramiko.SFTP_PERMISSION_DENIED
        
        path = self._realpath(path)
        logger.info(f"User {self.current_user} opening file: {path} with flags: {flags}")
        
        try:
            binary_flag = getattr(os, 'O_BINARY', 0)
            flags |= binary_flag
            mode = getattr(attr, 'st_mode', None)
            if mode is not None:
                fd = os.open(path, flags, mode)
            else:
                # Create with more permissive mode for user files
                fd = os.open(path, flags, 0o644)
        except OSError as e:
            logger.error(f"Failed to open {path}: {e}")
            return paramiko.SFTP_FAILURE

        if flags & os.O_WRONLY:
            if flags & os.O_APPEND:
                fstr = 'ab'
            else:
                fstr = 'wb'
            f = ProductionSFTPHandle(flags, self.current_user)
            f.filename = path
            f.writefile = os.fdopen(fd, fstr)
            logger.info(f"Opened file for writing: {path}")
            return f
        elif flags & os.O_RDWR:
            if flags & os.O_APPEND:
                fstr = 'a+b'
            else:
                fstr = 'r+b'
            f = ProductionSFTPHandle(flags, self.current_user)
            f.filename = path
            f.readfile = os.fdopen(fd, fstr)
            f.writefile = f.readfile
            logger.info(f"Opened file for read/write: {path}")
            return f
        else:
            # O_RDONLY (== 0)
            f = ProductionSFTPHandle(flags, self.current_user)
            f.filename = path
            f.readfile = os.fdopen(fd, 'rb')
            logger.info(f"Opened file for reading: {path}")
            return f

    def remove(self, path):
        if not self._check_permission('delete'):
            return paramiko.SFTP_PERMISSION_DENIED
            
        path = self._realpath(path)
        try:
            os.remove(path)
            logger.info(f"User {self.current_user} removed file: {path}")
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def rename(self, oldpath, newpath):
        if not self._check_permission('rename'):
            return paramiko.SFTP_PERMISSION_DENIED
            
        oldpath = self._realpath(oldpath)
        newpath = self._realpath(newpath)
        try:
            os.rename(oldpath, newpath)
            logger.info(f"User {self.current_user} renamed: {oldpath} -> {newpath}")
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def mkdir(self, path, attr):
        if not self._check_permission('mkdir'):
            return paramiko.SFTP_PERMISSION_DENIED
            
        path = self._realpath(path)
        logger.info(f"User {self.current_user} creating directory: {path}")
        try:
            os.makedirs(path, exist_ok=True)
            logger.info(f"Successfully created directory: {path}")
        except OSError as e:
            logger.error(f"Failed to create directory {path}: {e}")
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def rmdir(self, path):
        if not self._check_permission('rmdir'):
            return paramiko.SFTP_PERMISSION_DENIED
            
        path = self._realpath(path)
        try:
            os.rmdir(path)
            logger.info(f"User {self.current_user} removed directory: {path}")
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK


class ProductionServer(paramiko.ServerInterface):
    def __init__(self, user_manager: UserManager):
        self.event = threading.Event()
        self.user_manager = user_manager
        self.authenticated_user = None

    def check_channel_request(self, kind, chanid):
        if kind == 'session':
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_password(self, username, password):
        """Enhanced password authentication with user management."""
        if self.user_manager.authenticate_password(username, password):
            self.authenticated_user = username
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def check_auth_publickey(self, username, key):
        """SSH key authentication."""
        if self.user_manager.authenticate_key(username, key):
            self.authenticated_user = username
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        """Return allowed authentication methods."""
        if self.user_manager.user_exists(username):
            return 'password,publickey'
        return 'none'

    def check_channel_shell_request(self, channel):
        return False

    def check_channel_pty_request(self, channel, term, width, height, pixelwidth, pixelheight, modes):
        return False


def handle_client(client, host_key, user_manager):
    try:
        transport = paramiko.Transport(client)
        transport.add_server_key(host_key)
        
        # Create server interface with user manager
        server = ProductionServer(user_manager)
        
        # Setup SFTP subsystem - fix the lambda issue
        transport.set_subsystem_handler('sftp', SFTPServer, sftp_si=ProductionSFTPServer, user_manager=user_manager)
        transport.start_server(server=server)

        # Wait for auth
        channel = transport.accept(20)
        if channel is None:
            logger.warning("No channel")
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


def main():
    # Use centralized settings
    from mm_bronze.common.config import settings
    
    # Initialize user manager with settings
    user_manager = UserManager(settings)
    
    # Start async components
    async def start_async_components():
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
    
    # Start async components in background thread
    def run_async():
        asyncio.run(start_async_components())
    
    async_thread = threading.Thread(target=run_async, daemon=True)
    async_thread.start()
    
    # Load or generate host key
    host_key = load_host_key(settings.sftp_host_key_path)
    
    # Create uploads directory
    os.makedirs(settings.sftp_upload_root, exist_ok=True)
    os.chdir(settings.sftp_upload_root)  # Change to uploads directory
    logger.info(f"Working directory: {os.getcwd()}")

    # Setup socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((settings.sftp_host, settings.sftp_port))
    except Exception as e:
        logger.error(f"Bind failed: {e}")
        return

    try:
        sock.listen(100)
        logger.info(f"Listening for connection on {settings.sftp_host}:{settings.sftp_port}...")
        logger.info(f"Configured users: {list(user_manager.users.keys())}")
        
        while True:
            client, addr = sock.accept()
            logger.info(f"Got connection from {addr}")
            
            # Handle client in background thread
            client_thread = threading.Thread(
                target=handle_client,
                args=(client, host_key, user_manager),
                daemon=True
            )
            client_thread.start()
            
    except Exception as e:
        logger.error(f"Listen/accept failed: {e}")
    finally:
        sock.close()


def load_host_key(host_key_path: str) -> paramiko.PKey:
    """Load or generate SSH host key."""
    if host_key_path and os.path.exists(host_key_path):
        try:
            # Try to load existing key
            if host_key_path.endswith('.pub'):
                # Remove .pub extension for private key
                private_key_path = host_key_path[:-4]
            else:
                private_key_path = host_key_path
            
            if os.path.exists(private_key_path):
                host_key = paramiko.RSAKey.from_private_key_file(private_key_path)
                logger.info(f"Loaded host key from {private_key_path}")
                return host_key
        except Exception as e:
            logger.warning(f"Failed to load host key from {host_key_path}: {e}")
    
    # Generate new key in writable location
    logger.info("Generating new RSA host key")
    host_key = paramiko.RSAKey.generate(2048)
    
    # Try to save key if path specified and directory is writable
    if host_key_path:
        try:
            # Check if directory is writable
            private_key_path = host_key_path.replace('.pub', '')
            key_dir = os.path.dirname(private_key_path)
            
            # Test if directory is writable
            test_file = os.path.join(key_dir, '.write_test')
            try:
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                
                # Directory is writable, save key
                host_key.write_private_key_file(private_key_path)
                
                # Save public key
                with open(f"{private_key_path}.pub", 'w') as f:
                    f.write(f"{host_key.get_name()} {host_key.get_base64()}\n")
                
                logger.info(f"Saved host key to {private_key_path}")
            except (OSError, PermissionError):
                # Directory not writable (e.g., read-only mount), use temp location
                temp_key_path = f"/tmp/ssh_host_rsa_key"
                host_key.write_private_key_file(temp_key_path)
                logger.info(f"Saved host key to temporary location: {temp_key_path}")
                
        except Exception as e:
            logger.warning(f"Failed to save host key: {e}")
    
    return host_key


if __name__ == "__main__":
    main()