from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict, field_validator
from typing import Any, Dict
import logging

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """
    Shared application settings loaded from environment variables.
    """

    # — Kafka —
    kafka_servers: str = Field(..., description="Comma-separated bootstrap servers")

    kafka_bronze_api_topic: str = Field(
        ..., description="Topic for raw ingestion from the API"
    )

    kafka_bronze_api_group: str = Field(
        ..., description="Consumer group for ingestion from the API"
    )

    kafka_bronze_sftp_topic: str = Field(
        ..., description="Topic for notifying that data has been ingested through SFTP"
    )

    kafka_bronze_sftp_group: str = Field(
        ..., description="Consumer group for handling SFTP files"
    )

    kafka_cfg_advertised_listeners: str = Field(
        ..., description="Kafka advertised listeners configuration"
    )

    # — Postgres —
    postgres_dsn: str = Field(
        ...,
        description="Full DSN, e.g. postgres://user:pw@host/db",
    )

    db_min_size: int = Field(
        ...,
        description="Number of connections the asyncpg Pool is initiated with.",
    )

    db_max_size: int = Field(
        ...,
        description="Max number of connections allowed by the asyncpg Pool.",
    )

    # — Filesystem —
    raw_storage_url: str = Field(
        ...,
        description="Storage location where raw data will be written",
    )

    # — SFTP Server Configuration —
    sftp_host: str = Field(default="0.0.0.0", description="SFTP server bind address")

    sftp_port: int = Field(default=2222, description="SFTP server port", ge=1, le=65535)

    sftp_upload_root: str = Field(
        default="/uploads", description="SFTP upload root directory"
    )

    sftp_host_key_path: str = Field(
        default="/app/keys/ssh_host_rsa_key", description="Path to SSH host private key"
    )

    sftp_keys_dir: str = Field(
        default="/app/keys/users",
        description="Directory containing user SSH public keys",
    )

    sftp_users: str = Field(
        default="alice:secret:read+write",
        description="SFTP users configuration. Format: user1:pass1:perm1+perm2|user2:pass2:perm1+perm2",
    )

    # Pydantic V2 configuration
    model_config = ConfigDict(
        case_sensitive=False,  # env var names are case-insensitive
        env_file=".env",
        env_file_encoding="utf-8",
        env_names={  # explicit mapping to environment variables
            "kafka_servers": "KAFKA_SERVERS",
            "kafka_bronze_api_topic": "KAFKA_BRONZE_API_TOPIC",
            "kafka_bronze_api_group": "KAFKA_BRONZE_API_GROUP",
            "kafka_bronze_sftp_topic": "KAFKA_BRONZE_SFTP_TOPIC",
            "kafka_bronze_sftp_group": "KAFKA_BRONZE_SFTP_GROUP",
            "kafka_cfg_advertised_listeners": "KAFKA_CFG_ADVERTISED_LISTENERS",
            "postgres_dsn": "POSTGRES_DSN",
            "raw_storage_url": "RAW_STORAGE_URL",
            "db_min_size": "DB_MIN_SIZE",
            "db_max_size": "DB_MAX_SIZE",
            "watch_root": "WATCH_ROOT",
            "sftp_host": "SFTP_HOST",
            "sftp_port": "SFTP_PORT",
            "sftp_upload_root": "SFTP_UPLOAD_ROOT",
            "sftp_host_key_path": "SFTP_HOST_KEY_PATH",
            "sftp_keys_dir": "SFTP_KEYS_DIR",
            "sftp_users": "SFTP_USERS",
        },
    )

    @field_validator("sftp_users")
    def validate_sftp_users(cls, v):
        """Validate SFTP users configuration format."""
        if not v:
            raise ValueError("SFTP_USERS cannot be empty")

        try:
            users = cls.parse_sftp_users(v)
            if not users:
                raise ValueError("No valid users found in SFTP_USERS")
            logger.info(f"Validated {len(users)} SFTP users")
            return v
        except Exception as e:
            raise ValueError(f"Invalid SFTP_USERS format: {e}")

    @staticmethod
    def parse_sftp_users(users_str: str) -> Dict[str, Dict[str, Any]]:
        """
        Parse SFTP users string into structured data.

        Format: user1:password1:perm1+perm2+perm3|user2:password2:perm1+perm2

        Returns:
            Dict[username, {'password': str, 'permissions': Set[str]}]
        """
        users = {}
        valid_permissions = {"read", "write", "delete"}

        for user_spec in users_str.split("|"):
            user_spec = user_spec.strip()
            if not user_spec:
                continue

            parts = user_spec.split(":")
            if len(parts) < 2:
                raise ValueError(f"Invalid user specification: {user_spec}")

            username = parts[0].strip()
            password = parts[1].strip()

            if not username or not password:
                raise ValueError(f"Username and password cannot be empty: {user_spec}")

            # Parse permissions
            permissions = set()
            if len(parts) > 2 and parts[2].strip():
                perm_list = parts[2].strip().split("+")
                for perm in perm_list:
                    perm = perm.strip()
                    if perm not in valid_permissions:
                        raise ValueError(
                            f"Invalid permission '{perm}' for user {username}. "
                            f"Valid permissions: {valid_permissions}"
                        )
                    permissions.add(perm)
            else:
                # Default permissions if none specified
                permissions = {"read", "write"}

            if username in users:
                raise ValueError(f"Duplicate user: {username}")

            users[username] = {"password": password, "permissions": permissions}

        return users

    def get_sftp_users(self) -> Dict[str, Dict[str, Any]]:
        """Get parsed SFTP users configuration."""
        return self.parse_sftp_users(self.sftp_users)


def __getattr__(name: str) -> Any:
    if name == "settings":
        return Settings()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
