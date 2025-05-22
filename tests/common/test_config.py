#!/usr/bin/env python
import sys
from dotenv import load_dotenv
load_dotenv(".env")

from mm_bronze.common.config import settings

print("--- configuration values ---")
print(f"KAFKA_SERVERS       = {settings.kafka_servers!r}")
print(f"KAFKA_API_RAW_TOPIC = {settings.kafka_api_raw_topic!r}")
print(f"POSTGRES_DSN       = {settings.postgres_dsn!r}")
print(f"DB_MIN_SIZE        = {settings.db_min_size!r}")
print(f"DB_MAX_SIZE        = {settings.db_max_size!r}")
print(f"RAW_STORAGE_URL    = {settings.raw_storage_url!r}")

# Exit non-zero if anything is missing
missing = [name for name, val in vars(settings).items() if val is None]
if missing:
    print("Missing settings:", missing, file=sys.stderr)
    sys.exit(1)
