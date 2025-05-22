from pydantic_settings import BaseSettings

from pydantic import Field, ConfigDict
from typing import Any


class Settings(BaseSettings):
    """
    Shared application settings loaded from environment variables.
    """

    # — Kafka —
    kafka_servers: str = Field(
        ..., description="Comma-separated bootstrap servers"
    )
    kafka_api_raw_group: str = Field(
        ..., description="Consumer group for ingestion from the API"
    )
    kafka_api_raw_topic: str = Field(
        ..., description="Topic for raw ingestion from the API"
    )
    
    kafka_cfg_advertised_listeners: str = Field(
        ..., description="Consumer group for raw sink from the API"
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
        description="Max number of connections allowed by the aysncpg Pool.",
    )

    # — Filesystem —
    raw_storage_url: str = Field(
        ...,
        description="Storage location where raw data will be written",
    )

    # Pydantic V2 configuration: no env_file so missing vars cause errors
    model_config = ConfigDict(
        case_sensitive=False,  # env var names are case-insensitive
        env_file=".env",
        env_file_encoding="utf-8",
        env_names={  # explicit mapping to environment variables
            "kafka_servers": "KAFKA_SERVERS",
            "kafka_api_raw_topic": "KAFKA_API_RAW_TOPIC",
            "kafka_api_raw_group": "KAFKA_API_RAW_GROUP",
            "kafka_cfg_advertised_listeners": "KAFKA_CFG_ADVERTISED_LISTENERS",
            "postgres_dsn": "POSTGRES_DSN",
            "raw_storage_url": "RAW_STORAGE_URL",
            "db_min_size": "DB_MIN_SIZE",
            "db_max_size": "DB_MAX_SIZE",
        },
    )


def __getattr__(name: str) -> Any:
    if name == "settings":
        return Settings()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")