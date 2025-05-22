CREATE SCHEMA IF NOT EXISTS ingestion;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS ingestion.raw_api_ingest (
    object_id       UUID PRIMARY KEY,                -- supplied by API
    format          TEXT      NOT NULL,              -- e.g. 'fhir', 'hl7', 'note'
    content_type    TEXT,                            -- e.g. 'json', 'xml', 'text'
    subtype         TEXT      NOT NULL,              -- e.g. 'Patient', 'Observation', or generic type
    data_version    TEXT,                            -- e.g. 'r4', 'stu3', version tag for non-FHIR types
    storage_path    TEXT      NOT NULL,              -- URI or path to raw payload
    fingerprint     BYTEA     NOT NULL UNIQUE,       -- ShA256 hash of the validated payload to confirm uniqueness
    received_at     TIMESTAMPTZ DEFAULT now()        -- ingestion timestamp
);


CREATE TABLE IF NOT EXISTS ingestion.ingestion_log (
    log_id          BIGSERIAL PRIMARY KEY,
    object_id       UUID,
    log_time        TIMESTAMPTZ DEFAULT now(),       -- event timestamp
    status          TEXT      NOT NULL,              -- e.g. 'duplicated', 'ingested', 'failed'
    message         TEXT                             -- error or info message
);