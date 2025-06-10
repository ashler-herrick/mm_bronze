# mm_bronze

A healthcare data ingestion platform designed for processing FHIR, HL7, and other healthcare data formats through REST API and SFTP interfaces.

## Overview

mm_bronze is a microservices-based data ingestion platform that provides:

- **Multi-Protocol Ingestion**: REST API and SFTP server for flexible data intake
- **Event-Driven Architecture**: Kafka-based messaging for reliable data processing
- **Pluggable Storage**: Support for local filesystem, S3, and other storage backends
- **Healthcare Focus**: Optimized for FHIR, HL7, and medical data formats
- **Metadata Tracking**: PostgreSQL-based tracking with global deduplication

## Architecture

The platform follows an event-driven microservices architecture:

```
[Data Sources]  →  [Ingestion Layer]  →  [Kafka]  →  [Storage Layer]  →  [Storage Backends]
                                              ↓
                                        [PostgreSQL]
                                        (Metadata)

Ingestion Methods:
• REST API (FastAPI) - HTTP/JSON ingestion
• SFTP Server - File upload ingestion

Data Flow:
1. Data arrives via REST API or SFTP upload
2. Ingestion services validate and publish to Kafka topics
3. Storage consumers process Kafka messages
4. Data persisted to configured storage backends
5. Metadata recorded in PostgreSQL for tracking
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ and uv package manager (optional, for development/testing)

### Installation

1. **Clone the repository**
   ```bash
   git clone git@github.com:ashler-herrick/mm_bronze.git
   cd mm_bronze
   ```

2. **Install dependencies**
   ```bash
   uv sync
   ```

3. **Create a .env file from the example.env**
   ```bash
   cp example.env .env
   ```

4. **Create a local directory for mounting the containers.**
   ```bash
   mkdir -p ./data/raw_storage
   ```
5. **Set up test SFTP keys (Optional)**
   ```bash
   ./scripts/setup_dev_keys.sh
   ```

6. **Start the platform** 
   ```bash
   docker-compose up --build -d
   ```

That's it! The platform will start with:
- REST API at `http://localhost:8000`
- SFTP server at `localhost:2222` 
- All supporting services (Kafka, PostgreSQL)

**Note:** The repository includes a development `.env` file with safe localhost configuration. For local customization, create a `.env` file to override specific values.

### Basic Usage

**REST API Ingestion:**
```bash
curl -X POST "http://localhost:8000/ingest/fhir/json/R4/bundle" \
  -H "Content-Type: application/json" \
  -d '{"resourceType": "Bundle", "type": "collection"}'
```

**SFTP Upload:**
```bash
# Using password authentication (user: alice, password: secret)
sftp -P 2222 alice@localhost

# Or using SSH keys (after running setup_dev_keys.sh)
sftp -P 2222 -i client_keys/alice_key alice@localhost

# Upload files to trigger automatic processing
```

## Services

### Ingestion API (`localhost:8000`)

FastAPI service providing REST endpoints for data ingestion:

- **Endpoint**: `POST /ingest/{format}/{content_type}/{version}/{subtype}`
- **Formats**: FHIR, HL7, custom healthcare formats
- **Features**: Payload validation, UUID generation, Kafka publishing, upload size limits

### SFTP Server (`localhost:2222`)

Custom SFTP implementation with automatic file processing:

- **Authentication**: SSH key-based user management
- **Monitoring**: Real-time file upload detection, upload size limits
- **Processing**: Automatic Kafka event publishing for uploaded files

### Storage Services

Background Kafka consumers that persist data to configured storage backends:

- **API Storage Consumer**: Processes REST API ingestion events
- **SFTP Storage Consumer**: Processes SFTP upload events
- **Storage Backends**: Local filesystem, S3, and other fsspec-supported systems

## Configuration

### Environment Variables

The repository includes a development `.env` file with safe localhost configuration. Key settings include:

```env
# Kafka Configuration (optimized for performance)
KAFKA_COMPRESSION_TYPE=none
KAFKA_MAX_MESSAGE_SIZE=8388608  # 8MB

# Database (points to Docker container)
POSTGRES_DSN=postgres://meta_user:meta_pass@postgres:5432/metadata

# SFTP (test credentials for development)
SFTP_USERS=alice:secret:read+write+delete

# Storage (local filesystem for development)
RAW_STORAGE_URL=file:///path/to/test/storage
```

For local customization, create a `.env` file to override specific values without affecting the shared development configuration.
You will need to update references
## Database Schema

PostgreSQL database "metadata" with schema "ingestion":

- **`raw_ingestion`**: Unified table for all ingestion methods
  - `ingestion_source`: Source method ('api', 'sftp', etc.)
  - `source_metadata`: JSONB field for method-specific metadata
  - Global deduplication via SHA-256 fingerprints

- **`ingestion_log`**: General ingestion logging

## Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Test specific modules
uv run pytest tests/common/
uv run pytest tests/ingestion/
uv run pytest tests/storage/

# Integration tests (requires running services)
uv run pytest tests/integration/
```

### Development Workflow

```bash
# After making code changes, restart with fresh volumes and rebuild
docker-compose down -v
docker-compose up --build

# For quick restart without rebuilding (no code changes)
docker-compose down -v
docker-compose up

# View logs from specific services
docker-compose logs -f ingest_api
docker-compose logs -f storage_api
```

### Individual Service Development

```bash
# Start individual services
docker-compose up ingest_api     # REST API only
docker-compose up ingest_sftp    # SFTP server only
docker-compose up storage_api    # API storage processor only
docker-compose up storage_sftp   # SFTP storage processor only
```

### Code Quality

```bash
# Linting
ruff check

# Formatting
ruff format
```

## Testing & Validation

### Utility Scripts

The platform includes several consolidated utility scripts:

- **`scripts/compression_toolkit.py`**: Unified compression analysis and benchmarking
  ```bash
  # Analyze compression ratios
  python scripts/compression_toolkit.py analyze --input /path/to/data
  
  # Run performance benchmarks
  python scripts/compression_toolkit.py benchmark --input /path/to/data
  
  # Validate configuration
  python scripts/compression_toolkit.py validate
  
  # Get recommendations
  python scripts/compression_toolkit.py recommend --workload high-throughput
  ```

- **`scripts/system_validator.py`**: Unified system validation and testing
  ```bash
  # Test file size limits
  python scripts/system_validator.py size-limits
  
  # Performance testing
  python scripts/system_validator.py performance --input /path/to/data
  
  # Integration checks
  python scripts/system_validator.py integration --quick
  
  # Configuration validation
  python scripts/system_validator.py config
  ```

- **`scripts/split_fhir_bundles.py`**: FHIR bundle splitting for testing
  ```bash
  # Split large FHIR bundles into individual resources
  python scripts/split_fhir_bundles.py --input /path/to/bundles --output /path/to/resources
  ```

- **`scripts/setup_dev_keys.sh`**: SSH key setup for SFTP development environment

## API Reference

### REST API Endpoints

**POST** `/ingest/{format}/{content_type}/{version}/{subtype}`

- **Parameters**:
  - `format`: Data format (fhir, hl7, custom)
  - `content_type`: Content type (json, xml)
  - `version`: Format version (R4, 2.9)
  - `subtype`: Data subtype (bundle, patient, observation)

- **Headers**:
  - `Content-Type`: application/json or application/xml

- **Response**: 
  ```json
  {
    "status": "accepted",
    "uuid": "12345678-1234-1234-1234-123456789abc",
    "timestamp": "2025-01-01T00:00:00Z"
  }
  ```

### SFTP Interface

- **Host**: localhost
- **Port**: 2222
- **Authentication**: 
  - Password: user `alice`, password `secret`
  - SSH key: `client_keys/alice_key` (after running `./scripts/setup_dev_keys.sh`)
- **Upload Directory**: Root directory (files uploaded here are automatically processed)
- **Supported Formats**: Any file format (processed based on extension/content)

## Supported Data Formats

- **FHIR R4**: JSON and XML bundles, individual resources
- **HL7 v2.x**: Standard HL7 messages
- **Custom Healthcare Formats**: Extensible format support
- **Bulk Data**: Large file and batch processing

## Storage Backends

Configured via fsspec, supporting:

- **Local Filesystem**: Default development setup
- **Amazon S3**: Production cloud storage
- **Azure Blob Storage**: Enterprise cloud storage
- **Google Cloud Storage**: Multi-cloud support
- **Custom Backends**: Any fsspec-compatible storage

### Log Monitoring

Service logs available via Docker Compose:
```bash
docker-compose logs -f ingest_api
docker-compose logs -f storage_api
```

## Troubleshooting

### Common Issues

1. **SFTP Connection Failed**
   - Verify SSH keys are properly set up: `./scripts/setup_dev_keys.sh`
   - Check SFTP service is running: `docker-compose ps ingest_sftp`

2. **Kafka Connection Issues**
   - Ensure Kafka is running: `docker-compose ps kafka`
   - Check Kafka logs: `docker-compose logs kafka`

3. **Storage Processing Delays**
   - Monitor Kafka consumer lag
   - Check storage backend connectivity
   - Verify filesystem permissions

4. **Database Connection Errors**
   - Verify PostgreSQL is running: `docker-compose ps postgres`
   - Check database credentials in `.env`

### Performance Tuning

1. **Kafka Optimization**
   - Keep compression disabled for maximum throughput
   - Adjust `KAFKA_MAX_MESSAGE_SIZE` for large payloads
   - Monitor consumer group lag

2. **Storage Optimization**
   - Use appropriate storage backend for your use case
   - Consider data partitioning strategies
   - Monitor disk I/O and network bandwidth

## Contributing

When contributing to the platform:

1. **Development Setup**: 
   ```bash
   git clone git@github.com:ashler-herrick/mm_bronze.git
   cd mm_bronze
   uv sync            
   docker-compose up  # Start the platform
   ```

2. **Code Quality**: Run `ruff check` and `ruff format` before committing

3. **Testing**: Ensure all tests pass with `uv run pytest`

4. **Documentation**: Update relevant documentation for new features

5. **Development Workflow**: Use `docker-compose down -v && docker-compose up --build` after code changes