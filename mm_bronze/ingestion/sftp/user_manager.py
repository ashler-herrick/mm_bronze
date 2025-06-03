"""
User management for SFTP server with password and SSH key authentication.
"""

import os
import base64
import logging
from pathlib import Path
from typing import Dict, Set
import paramiko

logger = logging.getLogger(__name__)


class UserManager:
    """Manages SFTP users with both password and SSH key authentication."""

    def __init__(self):
        """Initializes the UserManager by loading users and their SSH keys from the environment."""
        self.users: Dict[
            str, Dict
        ] = {}  # username -> {'password': str, 'keys': [PKey], 'permissions': set}
        self._load_users_from_env()
        self._load_ssh_keys()

    def _load_users_from_env(self):
        """Loads users from the SFTP_USERS environment variable.

        The expected format is:
            "user1:pass1:perm1+perm2|user2:pass2"

        If permissions are not provided, default to {'read', 'write'}.
        """
        # Format: "user1:pass1:perm1+perm2+perm3|user2:pass2:perm1+perm2"
        # Separators: | between users, : between fields, + between permissions
        # Example: "alice:secret:read+write+delete|readonly:readonly123:read|admin:admin123:read+write+delete"
        users_env = os.getenv("SFTP_USERS", "alice:secret:read+write")

        for user_spec in users_env.split("|"):
            user_spec = user_spec.strip()
            if not user_spec:
                continue

            parts = user_spec.split(":")
            if len(parts) >= 2:
                username = parts[0].strip()
                password = parts[1].strip()
                permissions = (
                    set(parts[2].split("+")) if len(parts) > 2 else {"read", "write"}
                )

                self.users[username] = {
                    "password": password,
                    "keys": [],
                    "permissions": permissions,
                }
                logger.info(f"Loaded user {username} with permissions: {permissions}")
            else:
                logger.warning(f"Invalid user specification: {user_spec}")

    def _load_ssh_keys(self):
        """Loads SSH public keys from the directory specified by SFTP_KEYS_DIR.

        Keys should be named as <username>.pub and be in OpenSSH format.
        """
        keys_dir = Path(os.getenv("SFTP_KEYS_DIR", "/app/keys/users"))

        if not keys_dir.exists():
            logger.info(
                f"SSH keys directory {keys_dir} doesn't exist, skipping key loading"
            )
            return

        for username in self.users:
            user_key_file = keys_dir / f"{username}.pub"
            if user_key_file.exists():
                try:
                    with open(user_key_file, "r") as f:
                        key_data = f.read().strip()

                    # Parse the public key
                    parts = key_data.split()
                    if len(parts) >= 2:
                        key_type = parts[0]
                        key_blob = base64.b64decode(parts[1])

                        if key_type.startswith("ssh-rsa"):
                            key = paramiko.RSAKey(data=key_blob)
                        elif key_type.startswith("ssh-ed25519"):
                            key = paramiko.Ed25519Key(data=key_blob)
                        elif key_type.startswith("ecdsa-sha2"):
                            key = paramiko.ECDSAKey(data=key_blob)
                        else:
                            logger.warning(
                                f"Unsupported key type {key_type} for user {username}"
                            )
                            continue

                        self.users[username]["keys"].append(key)
                        logger.info(
                            f"Loaded SSH key for user {username} (type: {key_type})"
                        )

                except Exception as e:
                    logger.error(f"Failed to load SSH key for user {username}: {e}")

    def authenticate_password(self, username: str, password: str) -> bool:
        """Authenticates a user using a password.

        Args:
            username (str): The username to authenticate.
            password (str): The password to verify.

        Returns:
            bool: True if authentication is successful, False otherwise.
        """
        user = self.users.get(username)
        if user and user["password"] == password:
            logger.info(f"Password authentication successful for {username}")
            return True
        logger.warning(f"Password authentication failed for {username}")
        return False

    def authenticate_key(self, username: str, key: paramiko.PKey) -> bool:
        """Authenticates a user using an SSH public key.

        Args:
            username (str): The username to authenticate.
            key (paramiko.PKey): The public key to verify.

        Returns:
            bool: True if the key matches a stored key for the user, False otherwise.
        """
        user = self.users.get(username)
        if not user:
            logger.warning(f"Key authentication failed: unknown user {username}")
            return False

        for user_key in user["keys"]:
            if (
                key.get_name() == user_key.get_name()
                and key.asbytes() == user_key.asbytes()
            ):
                logger.info(f"SSH key authentication successful for {username}")
                return True

        logger.warning(f"SSH key authentication failed for {username}")
        return False

    def get_user_permissions(self, username: str) -> Set[str]:
        """Retrieves the set of permissions assigned to a user.

        Args:
            username (str): The username whose permissions to retrieve.

        Returns:
            Set[str]: A set of permissions (e.g., {'read', 'write'}), or an empty set if user is not found.
        """
        user = self.users.get(username)
        return user["permissions"] if user else set()

    def user_exists(self, username: str) -> bool:
        """Checks if a user exists in the system.

        Args:
            username (str): The username to check.

        Returns:
            bool: True if the user exists, False otherwise.
        """
        return username in self.users
