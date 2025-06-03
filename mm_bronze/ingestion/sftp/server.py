"""
SSH server interface for handling authentication and channel requests.
SFTP server interface with federated access and permission controls.
"""

import os
import threading
import logging
from typing import List, Any

from pathlib import Path
import paramiko
from paramiko import SFTPServerInterface, SFTPAttributes, SFTPHandle

from .user_manager import UserManager
from .handle import ProductionSFTPHandle


logger = logging.getLogger(__name__)


class ProductionServer(paramiko.ServerInterface):
    """
    SSH server interface with user authentication support.

    Some methods are defined here but not called in this code. That is on purpose because
    they are called by paramiko behind the scenes.
    """

    def __init__(self, user_manager: UserManager):
        """Initializes the server with a user manager for authentication.

        Args:
            user_manager (UserManager): Instance managing users and credentials.
        """
        self.event = threading.Event()
        self.user_manager = user_manager
        self.authenticated_user = None

    def check_channel_request(self, kind: str, chanid: int) -> int:
        """Handles incoming channel requests.

        Args:
            kind (str): Type of channel request.
            chanid (int): Channel ID.

        Returns:
            int: Channel open result (e.g., OPEN_SUCCEEDED or OPEN_FAILED).
        """
        if kind == "session":
            return paramiko.OPEN_SUCCEEDED
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_password(self, username: str, password: str) -> int:
        """Authenticates user via password.

        Args:
            username (str): Username.
            password (str): Password.

        Returns:
            int: AUTH_SUCCESSFUL if successful, else AUTH_FAILED.
        """
        if self.user_manager.authenticate_password(username, password):
            self.authenticated_user = username
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def check_auth_publickey(self, username: str, key: paramiko.PKey) -> int:
        """Authenticates user via SSH public key.

        Args:
            username (str): Username.
            key (paramiko.PKey): Public key object.

        Returns:
            int: AUTH_SUCCESSFUL if successful, else AUTH_FAILED.
        """

        if self.user_manager.authenticate_key(username, key):
            self.authenticated_user = username
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str) -> str:
        """Returns allowed authentication methods for a given user.

        Args:
            username (str): Username.

        Returns:
            str: Comma-separated list of allowed authentication methods.
        """
        if self.user_manager.user_exists(username):
            return "password,publickey"
        return "none"


class ProductionSFTPServer(SFTPServerInterface):
    """SFTP server implementation with user-based access control and federated directories."""

    def __init__(self, server, *args, user_manager=None, event_loop=None, **kwargs):
        """Initializes the SFTP server with federated root directory and user-specific controls.

        Args:
            server: The parent server instance.
            user_manager (UserManager, optional): User manager for access control.
            event_loop (asyncio.AbstractEventLoop, optional): Event loop for async operations.
            *args, **kwargs: Additional arguments passed to parent class
        """
        # Call parent constructor with remaining args
        super().__init__(server, *args, **kwargs)
        
        self.upload_root = os.getcwd()
        self.user_manager = user_manager
        self.current_user = getattr(server, "authenticated_user", None)
        self.event_loop = event_loop

        # Create user-specific directory (federated access)
        if self.current_user:
            self.user_root = os.path.join(self.upload_root, self.current_user)
            os.makedirs(self.user_root, exist_ok=True)
            logger.info(
                f"SFTP server root for user {self.current_user}: {self.user_root}"
            )
        else:
            self.user_root = self.upload_root
            logger.info(f"SFTP server root: {self.upload_root}")

    def _realpath(self, path: str) -> str:
        """Resolves an SFTP path relative to the user's home directory and checks for traversal attacks.

        Args:
            path (str): SFTP-relative path.

        Returns:
            str: Fully resolved, validated absolute path.

        Raises:
            paramiko.SFTPError: If path traversal is attempted.
        """
        # Remove leading slash and resolve relative to user's directory
        clean_path = path.lstrip("/")
        full_path = os.path.abspath(os.path.join(self.user_root, clean_path))

        # Security check: ensure path is within the user's directory
        try:
            Path(full_path).resolve().relative_to(Path(self.user_root).resolve())
        except ValueError:
            logger.warning(
                f"Path traversal attempt blocked: {path} by user {self.current_user}"
            )
            raise paramiko.SFTPError(paramiko.SFTP_PERMISSION_DENIED, "Access denied")

        return full_path

    def _check_permission(self, operation: str) -> bool:
        """Checks if the current user has permission to perform a given operation.

        Args:
            operation (str): Operation name (e.g., 'read', 'write').

        Returns:
            bool: True if permitted, False otherwise.
        """
        if not self.current_user or not self.user_manager:
            return False

        permissions = self.user_manager.get_user_permissions(self.current_user)

        # Map operations to required permissions
        permission_map = {
            "read": "read",
            "write": "write",
            "delete": "delete",
            "mkdir": "write",
            "rmdir": "delete",
            "rename": "write",
        }

        required_permission = permission_map.get(operation, "read")
        has_permission = required_permission in permissions

        if not has_permission:
            logger.warning(
                f"Permission denied: user {self.current_user} attempted {operation} "
                f"but only has permissions: {permissions}"
            )

        return has_permission

    def list_folder(self, path: str) -> List | int:
        """Lists contents of a folder.

        Args:
            path (str): Relative or absolute path to directory.

        Returns:
            list or int: List of SFTPAttributes on success, or error code.
        """
        if not self._check_permission("read"):
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

    def stat(self, path: str) -> int | paramiko.SFTPAttributes:
        """Gets file or directory stats.

        Args:
            path (str): File or directory path.

        Returns:
            paramiko.SFTPAttributes or int: File attributes or error code.
        """
        if not self._check_permission("read"):
            return paramiko.SFTP_PERMISSION_DENIED

        path = self._realpath(path)
        try:
            return SFTPAttributes.from_stat(os.stat(path))
        except OSError:
            return paramiko.SFTP_NO_SUCH_FILE

    def lstat(self, path: str) -> int | paramiko.SFTPAttributes:
        """Gets file or directory stats without following symlinks.

        Args:
            path (str): File or directory path.

        Returns:
            paramiko.SFTPAttributes or int: File attributes or error code.
        """
        if not self._check_permission("read"):
            return paramiko.SFTP_PERMISSION_DENIED

        path = self._realpath(path)
        try:
            return SFTPAttributes.from_stat(os.lstat(path))
        except OSError:
            return paramiko.SFTP_NO_SUCH_FILE

    def open(self, path: str, flags: int, attr: Any) -> SFTPHandle | int:
        """Opens a file with specified flags and attributes.

        Args:
            path (str): File path.
            flags (int): Open flags.
            attr: File attributes.

        Returns:
            SFTPHandle or int: File handle or error code.
        """
        # Check permissions based on file operation
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT):
            if not self._check_permission("write"):
                return paramiko.SFTP_PERMISSION_DENIED
        else:
            if not self._check_permission("read"):
                return paramiko.SFTP_PERMISSION_DENIED

        path = self._realpath(path)
        logger.info(
            f"User {self.current_user} opening file: {path} with flags: {flags}"
        )

        try:
            binary_flag = getattr(os, "O_BINARY", 0)
            flags |= binary_flag
            mode = getattr(attr, "st_mode", None)
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
                fstr = "ab"
            else:
                fstr = "wb"
            f = ProductionSFTPHandle(flags, self.current_user, self.event_loop)
            f.filename = path
            f.writefile = os.fdopen(fd, fstr)
            logger.info(f"Opened file for writing: {path}")
            return f
        elif flags & os.O_RDWR:
            if flags & os.O_APPEND:
                fstr = "a+b"
            else:
                fstr = "r+b"
            f = ProductionSFTPHandle(flags, self.current_user, self.event_loop)
            f.filename = path
            f.readfile = os.fdopen(fd, fstr)
            f.writefile = f.readfile
            logger.info(f"Opened file for read/write: {path}")
            return f
        else:
            # O_RDONLY (== 0)
            f = ProductionSFTPHandle(flags, self.current_user, self.event_loop)
            f.filename = path
            f.readfile = os.fdopen(fd, "rb")
            logger.info(f"Opened file for reading: {path}")
            return f

    def remove(self, path: str) -> int:
        """Removes a file.

        Args:
            path (str): Path to file.

        Returns:
            int: SFTP_OK or SFTP_FAILURE.
        """
        if not self._check_permission("delete"):
            return paramiko.SFTP_PERMISSION_DENIED

        path = self._realpath(path)
        try:
            os.remove(path)
            logger.info(f"User {self.current_user} removed file: {path}")
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def rename(self, oldpath: str, newpath: str) -> int:
        """Renames or moves a file or directory.

        Args:
            oldpath (str): Current path.
            newpath (str): New path.

        Returns:
            int: SFTP_OK or SFTP_FAILURE.
        """
        if not self._check_permission("rename"):
            return paramiko.SFTP_PERMISSION_DENIED

        oldpath = self._realpath(oldpath)
        newpath = self._realpath(newpath)
        try:
            os.rename(oldpath, newpath)
            logger.info(f"User {self.current_user} renamed: {oldpath} -> {newpath}")
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def mkdir(self, path: str, attr: SFTPAttributes) -> int:
        """Creates a directory.

        Args:
            path (str): Path to new directory.
            attr: Directory attributes.

        Returns:
            int: SFTP_OK or SFTP_FAILURE.
        """
        if not self._check_permission("mkdir"):
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

    def rmdir(self, path: str) -> int:
        """Removes a directory.

        Args:
            path (str): Path to directory.

        Returns:
            int: SFTP_OK or SFTP_FAILURE.
        """
        if not self._check_permission("rmdir"):
            return paramiko.SFTP_PERMISSION_DENIED

        path = self._realpath(path)
        try:
            os.rmdir(path)
            logger.info(f"User {self.current_user} removed directory: {path}")
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK


def load_host_key(host_key_path: str) -> paramiko.PKey:
    """Loads an existing SSH host key from file or generates a new one.

    If the key does not exist, a new RSA key is generated and optionally saved
    to the specified path.

    Args:
        host_key_path (str): Path to the host key file (or .pub for public key).

    Returns:
        paramiko.PKey: Loaded or generated host key.
    """
    if host_key_path and os.path.exists(host_key_path):
        try:
            # Try to load existing key
            if host_key_path.endswith(".pub"):
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

    # Generate new key
    logger.info("Generating new RSA host key")
    host_key = paramiko.RSAKey.generate(2048)

    # Save key if path specified and directory exists
    if host_key_path:
        try:
            # Ensure directory exists
            private_key_path = host_key_path.replace(".pub", "")
            key_dir = os.path.dirname(private_key_path)

            if key_dir and not os.path.exists(key_dir):
                os.makedirs(key_dir, mode=0o700, exist_ok=True)
                logger.info(f"Created key directory: {key_dir}")

            # Save private key
            host_key.write_private_key_file(private_key_path)

            # Save public key
            with open(f"{private_key_path}.pub", "w") as f:
                f.write(f"{host_key.get_name()} {host_key.get_base64()}\n")

            logger.info(f"Saved host key to {private_key_path}")
        except Exception as e:
            logger.warning(f"Failed to save host key: {e}")

    return host_key
