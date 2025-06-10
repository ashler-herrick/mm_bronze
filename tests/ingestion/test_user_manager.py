"""
Unit tests for SFTP UserManager.
"""

import os
import tempfile
import paramiko
from pathlib import Path
from unittest.mock import patch, MagicMock

from mm_bronze.ingestion.sftp.user_manager import UserManager


class TestUserManagerUserParsing:
    """Test UserManager user environment parsing."""

    def test_load_users_from_env_basic(self):
        """Test loading users from environment with basic format."""
        with patch.dict(os.environ, {"SFTP_USERS": "alice:secret"}):
            manager = UserManager()
            assert manager.user_exists("alice")
            assert manager.get_user_permissions("alice") == {"read", "write"}
            assert manager.authenticate_password("alice", "secret")

    def test_load_users_from_env_with_permissions(self):
        """Test loading users with custom permissions."""
        with patch.dict(os.environ, {"SFTP_USERS": "admin:pass:read+write+delete"}):
            manager = UserManager()
            assert manager.user_exists("admin")
            assert manager.get_user_permissions("admin") == {"read", "write", "delete"}

    def test_load_users_from_env_multiple_users(self):
        """Test loading multiple users from environment."""
        users_env = "alice:secret:read+write|bob:pass123:read|admin:admin:read+write+delete"
        with patch.dict(os.environ, {"SFTP_USERS": users_env}):
            manager = UserManager()

            # Test alice
            assert manager.user_exists("alice")
            assert manager.get_user_permissions("alice") == {"read", "write"}
            assert manager.authenticate_password("alice", "secret")

            # Test bob
            assert manager.user_exists("bob")
            assert manager.get_user_permissions("bob") == {"read"}
            assert manager.authenticate_password("bob", "pass123")

            # Test admin
            assert manager.user_exists("admin")
            assert manager.get_user_permissions("admin") == {"read", "write", "delete"}
            assert manager.authenticate_password("admin", "admin")

    def test_load_users_from_env_empty_spec(self):
        """Test handling empty user specifications."""
        with patch.dict(os.environ, {"SFTP_USERS": "alice:secret||bob:pass"}):
            manager = UserManager()
            assert manager.user_exists("alice")
            assert manager.user_exists("bob")

    def test_load_users_from_env_invalid_spec(self):
        """Test handling invalid user specifications."""
        with patch.dict(os.environ, {"SFTP_USERS": "invalidspec|alice:secret"}):
            manager = UserManager()
            assert not manager.user_exists("invalidspec")
            assert manager.user_exists("alice")


class TestUserManagerAuthentication:
    """Test UserManager authentication methods."""

    def setup_method(self):
        """Set up test manager with known users."""
        with patch.dict(os.environ, {"SFTP_USERS": "alice:secret:read+write|bob:test123:read"}):
            self.manager = UserManager()

    def test_authenticate_password_success(self):
        """Test successful password authentication."""
        assert self.manager.authenticate_password("alice", "secret")
        assert self.manager.authenticate_password("bob", "test123")

    def test_authenticate_password_failure_wrong_password(self):
        """Test password authentication with wrong password."""
        assert not self.manager.authenticate_password("alice", "wrongpass")
        assert not self.manager.authenticate_password("bob", "wrongpass")

    def test_authenticate_password_failure_unknown_user(self):
        """Test password authentication with unknown user."""
        assert not self.manager.authenticate_password("unknown", "anypass")

    def test_authenticate_key_unknown_user(self):
        """Test key authentication with unknown user."""
        mock_key = MagicMock(spec=paramiko.RSAKey)
        assert not self.manager.authenticate_key("unknown", mock_key)

    def test_user_exists(self):
        """Test user_exists method."""
        assert self.manager.user_exists("alice")
        assert self.manager.user_exists("bob")
        assert not self.manager.user_exists("unknown")

    def test_get_user_permissions(self):
        """Test get_user_permissions method."""
        assert self.manager.get_user_permissions("alice") == {"read", "write"}
        assert self.manager.get_user_permissions("bob") == {"read"}
        assert self.manager.get_user_permissions("unknown") == set()


class TestUserManagerSSHKeys:
    """Test UserManager SSH key functionality."""

    def test_load_ssh_keys_no_directory(self):
        """Test SSH key loading when directory doesn't exist."""
        with patch.dict(os.environ, {"SFTP_USERS": "alice:secret", "SFTP_KEYS_DIR": "/nonexistent"}):
            manager = UserManager()
            # Should not raise exception, just skip key loading
            assert manager.user_exists("alice")
            assert len(manager.users["alice"]["keys"]) == 0

    def test_load_ssh_keys_with_valid_rsa_key(self):
        """Test loading a valid RSA SSH key."""
        # Create a temporary directory and key file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Generate a test RSA key pair
            key = paramiko.RSAKey.generate(2048)

            # Create public key file
            key_file = Path(temp_dir) / "alice.pub"
            with open(key_file, "w") as f:
                f.write(f"ssh-rsa {key.get_base64()} test@example.com\n")

            with patch.dict(os.environ, {"SFTP_USERS": "alice:secret", "SFTP_KEYS_DIR": temp_dir}):
                manager = UserManager()

                assert manager.user_exists("alice")
                assert len(manager.users["alice"]["keys"]) == 1

                # Test key authentication
                assert manager.authenticate_key("alice", key)

    def test_authenticate_key_mismatch(self):
        """Test key authentication with non-matching key."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Generate two different RSA keys
            stored_key = paramiko.RSAKey.generate(2048)
            different_key = paramiko.RSAKey.generate(2048)

            # Create public key file with stored_key
            key_file = Path(temp_dir) / "alice.pub"
            with open(key_file, "w") as f:
                f.write(f"ssh-rsa {stored_key.get_base64()} test@example.com\n")

            with patch.dict(os.environ, {"SFTP_USERS": "alice:secret", "SFTP_KEYS_DIR": temp_dir}):
                manager = UserManager()

                # Test with stored key (should work)
                assert manager.authenticate_key("alice", stored_key)

                # Test with different key (should fail)
                assert not manager.authenticate_key("alice", different_key)

    def test_load_ssh_keys_invalid_key_format(self):
        """Test handling of invalid SSH key format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create invalid key file
            key_file = Path(temp_dir) / "alice.pub"
            with open(key_file, "w") as f:
                f.write("invalid key format\n")

            with patch.dict(os.environ, {"SFTP_USERS": "alice:secret", "SFTP_KEYS_DIR": temp_dir}):
                manager = UserManager()

                # Should still create user but with no keys
                assert manager.user_exists("alice")
                assert len(manager.users["alice"]["keys"]) == 0
