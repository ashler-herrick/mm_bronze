CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS ingestion.raw_ingestion (
    object_id          UUID PRIMARY KEY,             -- UUID for this ingestion record
    ingestion_source   TEXT      NOT NULL,           -- 'api', 'sftp', 'webdav', 'bulk', etc.
    format             TEXT,                         -- e.g. 'fhir', 'hl7', 'dicom', 'document'
    content_type       TEXT,                         -- e.g. 'json', 'xml', 'text', 'binary'
    subtype            TEXT,                         -- e.g. 'Patient', 'Observation', document category
    data_version       TEXT,                         -- e.g. 'r4', 'stu3', version tag
    storage_path       TEXT      NOT NULL,           -- URI or path to raw payload
    fingerprint        BYTEA     NOT NULL,           -- SHA256 hash of payload for deduplication
    received_at        TIMESTAMPTZ DEFAULT now(),    -- ingestion timestamp
    source_metadata    JSONB                         -- Method-specific metadata (username, endpoint, etc.)
);

-- Minimal indexes optimized for high-frequency inserts
-- Only fingerprint needs uniqueness constraint for deduplication
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_ingestion_fingerprint 
    ON ingestion.raw_ingestion (fingerprint);

-- Single composite index for most common query patterns
-- Ordered by selectivity: source first (most selective), then time-based queries
CREATE INDEX IF NOT EXISTS idx_raw_ingestion_source_received 
    ON ingestion.raw_ingestion (ingestion_source, received_at DESC);



CREATE TABLE IF NOT EXISTS ingestion.ingestion_log (
    log_id          BIGSERIAL PRIMARY KEY,
    object_id       UUID,
    log_time        TIMESTAMPTZ DEFAULT now(),       -- event timestamp
    status          TEXT      NOT NULL,              -- e.g. 'duplicated', 'ingested', 'failed'
    message         TEXT                             -- error or info message
);