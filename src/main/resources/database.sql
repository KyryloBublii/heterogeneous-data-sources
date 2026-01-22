-- =====================================================================
-- SCHEMAS
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS auth;
CREATE SCHEMA IF NOT EXISTS integration;


-- =====================================================================
-- SERVICE ROLE FOR PYTHON INGESTION WRAPPER
-- =====================================================================
DO
$$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_roles WHERE rolname = 'ingestion_app'
    ) THEN
        CREATE ROLE ingestion_app LOGIN PASSWORD 'ingestion_app_password';
    END IF;
END;
$$;

DO
$$
DECLARE
    target_db TEXT := current_database();
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO ingestion_app', target_db);
END;
$$;

GRANT USAGE ON SCHEMA auth TO ingestion_app;
GRANT USAGE ON SCHEMA integration TO ingestion_app;
GRANT SELECT ON ALL TABLES IN SCHEMA auth TO ingestion_app;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA integration TO ingestion_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA integration TO ingestion_app;


-- =====================================================================
-- ENUM TYPES (put in integration schema for clarity)
-- =====================================================================
CREATE TYPE integration.source_type
    AS
    ENUM('CSV', 'JSON', 'DB');

CREATE TYPE integration.connector_role
    AS
    ENUM('SOURCE', 'DESTINATION');

CREATE TYPE integration.source_status
    AS
    ENUM('ACTIVE', 'PAUSED');

CREATE TYPE integration.run_status
    AS
    ENUM('QUEUED', 'RUNNING', 'SUCCESS', 'FAILED');

CREATE TYPE integration.data_type
    AS
    ENUM('TEXT', 'NUMERIC', 'BOOLEAN', 'TIMESTAMP', 'JSON');

CREATE TYPE integration.dataset_status
    AS
    ENUM('ACTIVE', 'PAUSED', 'FINISHED');

CREATE TYPE integration.transform_type
    AS
    ENUM('NONE', 'LOWERCASE', 'UPPERCASE', 'TRIM', 'INT', 'FLOAT');


-- =====================================================================
-- AUTH.SCHEMA  (users only)
-- =====================================================================
CREATE TABLE IF NOT EXISTS auth."user" (
                                           user_id        	 	BIGSERIAL PRIMARY KEY,
                                           user_uid        	 	VARCHAR(40) UNIQUE NOT NULL,
    name           	 	VARCHAR(60) NOT NULL,
    email         	 	VARCHAR(60) NOT NULL UNIQUE,
    password_hash 	 	TEXT NOT NULL,
    created_at    	 	TIMESTAMP NOT NULL DEFAULT now()
    );

-- =====================================================================
-- INTEGRATION CORE (sources, runs, raw)
-- =====================================================================
CREATE TABLE IF NOT EXISTS integration.source (
    source_id                     BIGSERIAL PRIMARY KEY,
    source_uid                    VARCHAR(40) UNIQUE NOT NULL,
    user_id                       BIGINT NOT NULL REFERENCES auth."user"(user_id) ON DELETE CASCADE,
    dataset_id                    BIGINT     REFERENCES integration.dataset(dataset_id) ON DELETE CASCADE,
    name                          VARCHAR(60) NOT NULL,
    type                          integration.source_type NOT NULL,
    role                          integration.connector_role NOT NULL DEFAULT 'SOURCE',
    config                        JSONB NOT NULL,
    status                        integration.source_status NOT NULL DEFAULT 'ACTIVE',
    created_at                    TIMESTAMP NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMP NOT NULL DEFAULT now()
    );

CREATE TABLE IF NOT EXISTS integration.ingestion_run (
    ingestion_id                   BIGSERIAL PRIMARY KEY,
    ingestion_uid                  VARCHAR(40) UNIQUE NOT NULL,
    dataset_id                     BIGINT     REFERENCES integration.dataset(dataset_id) ON DELETE CASCADE,
    source_id                      BIGINT NOT NULL REFERENCES integration.source(source_id) ON DELETE CASCADE,
    destination_id                 BIGINT     REFERENCES integration.source(source_id) ON DELETE SET NULL,
    run_status                     integration.run_status NOT NULL DEFAULT 'QUEUED',
    started_at                     TIMESTAMP,
    ended_at                       TIMESTAMP,
    rows_read                      INT DEFAULT 0,
    rows_stored                    INT DEFAULT 0,
    error_message                  TEXT
    );

CREATE TABLE IF NOT EXISTS integration.raw_event (
    raw_event_id                   BIGSERIAL PRIMARY KEY,
    raw_event_uid                  VARCHAR(40) UNIQUE NOT NULL,
    dataset_id                     BIGINT     REFERENCES integration.dataset(dataset_id) ON DELETE CASCADE,
    source_id                      BIGINT NOT NULL REFERENCES integration.source(source_id) ON DELETE CASCADE,
    ingestion_run_id               BIGINT     REFERENCES integration.ingestion_run(ingestion_id) ON DELETE SET NULL,
    payload                        JSONB NOT NULL,
    payload_hash                   TEXT,                                     -- add if you dedupe
    created_at                     TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT raw_event_dedupe UNIQUE NULLS NOT DISTINCT (source_id, payload_hash)
    );

-- =====================================================================
-- DATASETS (domain-agnostic global schema)
-- =====================================================================
CREATE TABLE IF NOT EXISTS integration.dataset (
    dataset_id                     BIGSERIAL PRIMARY KEY,
    dataset_uid                    VARCHAR(40) UNIQUE NOT NULL,
    user_id                        BIGINT NOT NULL REFERENCES auth."user"(user_id) ON DELETE CASCADE,
    name                           VARCHAR(60) NOT NULL,
    description                    TEXT,
    primary_record_type            TEXT,
    status                         integration.dataset_status NOT NULL DEFAULT 'ACTIVE',
    created_at                     TIMESTAMP NOT NULL DEFAULT now(),
    updated_at                     TIMESTAMP
    );

CREATE TABLE IF NOT EXISTS integration.dataset_field (
                                                         dataset_field_id 		BIGSERIAL PRIMARY KEY,
                                                         dataset_field_uid     VARCHAR(40) UNIQUE NOT NULL,
    dataset_id       		BIGINT NOT NULL REFERENCES integration.dataset(dataset_id) ON DELETE CASCADE,
    name             		VARCHAR(60) NOT NULL,
    dtype            		integration.data_type NOT NULL,
    is_nullable      		BOOLEAN NOT NULL DEFAULT TRUE,
    is_unique        		BOOLEAN NOT NULL DEFAULT FALSE,
    default_expr     		TEXT,
    position         		INT NOT NULL DEFAULT 0,
    CONSTRAINT dataset_field_unique_name UNIQUE (dataset_id, name)
    );

ALTER TABLE IF EXISTS integration.dataset_mapping DROP CONSTRAINT IF EXISTS dataset_mapping_uniq;

CREATE TABLE IF NOT EXISTS integration.dataset_mapping (
    dataset_mapping_id   BIGSERIAL PRIMARY KEY,
    dataset_mapping_uid   VARCHAR(40) UNIQUE NOT NULL,
    dataset_id           BIGINT NOT NULL REFERENCES integration.dataset(dataset_id) ON DELETE CASCADE,
    source_id            BIGINT NOT NULL REFERENCES integration.source(source_id) ON DELETE CASCADE,
    dataset_field_id     BIGINT NOT NULL REFERENCES integration.dataset_field(dataset_field_id) ON DELETE CASCADE,
    src_json_path        TEXT   NOT NULL,
    src_path             TEXT   NOT NULL DEFAULT '',
    transform_type       integration.transform_type NOT NULL DEFAULT 'NONE',
    transform_sql        TEXT,
    required             BOOLEAN NOT NULL DEFAULT FALSE,
    priority             INT     NOT NULL DEFAULT 0
    );

CREATE TABLE IF NOT EXISTS integration.transform_run (
                                                         transform_run_id 		BIGSERIAL PRIMARY KEY,
                                                         transform_run_uid     VARCHAR(40) UNIQUE NOT NULL,
    dataset_id       		BIGINT NOT NULL REFERENCES integration.dataset(dataset_id) ON DELETE CASCADE,
    started_at       		TIMESTAMP NOT NULL DEFAULT now(),
    ended_at         		TIMESTAMP,
    run_status       		integration.run_status NOT NULL DEFAULT 'RUNNING',
    rows_in          		INT DEFAULT 0,
    rows_out         		INT DEFAULT 0,
    error_message    		TEXT
    );

CREATE TABLE IF NOT EXISTS integration.unified_row (
    unified_row_id           BIGSERIAL PRIMARY KEY,
    unified_row_uid          VARCHAR(40) UNIQUE NOT NULL,
    dataset_id               BIGINT NOT NULL REFERENCES integration.dataset(dataset_id) ON DELETE CASCADE,
    source_id                BIGINT REFERENCES integration.source(source_id) ON DELETE SET NULL,
    record_key               TEXT,
    data                     JSONB NOT NULL,
    is_excluded              BOOLEAN NOT NULL DEFAULT false,
    observed_at              TIMESTAMP,
    ingested_at              TIMESTAMP NOT NULL DEFAULT now()
    );

-- =====================================================================
-- METADATA + CONNECTIONS (semantics)
-- =====================================================================

CREATE TABLE IF NOT EXISTS integration.connection (
    connection_id                  BIGSERIAL PRIMARY KEY,
    connection_uid                 VARCHAR(40) UNIQUE NOT NULL,
    dataset_id                     BIGINT     REFERENCES integration.dataset(dataset_id) ON DELETE CASCADE,
    source_id                      BIGINT NOT NULL REFERENCES integration.source(source_id) ON DELETE CASCADE,
    destination_id                 BIGINT     REFERENCES integration.source(source_id) ON DELETE SET NULL,
    relation                       VARCHAR(60),
    table_selection                JSONB,
    created_by                     TEXT,
    created_at                     TIMESTAMP NOT NULL DEFAULT now()
    );



CREATE TABLE IF NOT EXISTS integration.relationship (
    relationship_id           BIGSERIAL PRIMARY KEY,
    relationship_uid          VARCHAR(40) UNIQUE NOT NULL,
    source_id                 BIGINT NOT NULL REFERENCES integration.source(source_id) ON DELETE CASCADE,
    ingestion_run_id          BIGINT     REFERENCES integration.ingestion_run(ingestion_id) ON DELETE SET NULL,
    from_type                 VARCHAR(60) NOT NULL,
    from_id                   TEXT NOT NULL,
    to_type                   VARCHAR(60) NOT NULL,
    to_id                     TEXT NOT NULL,
    relation_type             VARCHAR(60) NOT NULL,
    payload                   JSONB,
    ingested_at               TIMESTAMP NOT NULL DEFAULT now()
    );

-- =====================================================================
-- INDEXES (performance)
-- =====================================================================
-- Common FKs and time filters
CREATE INDEX IF NOT EXISTS ix_ingestion_run_source    ON integration.ingestion_run(source_id, started_at);
CREATE INDEX IF NOT EXISTS ix_raw_event_source_time   ON integration.raw_event(source_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_unified_row_dataset_time ON integration.unified_row(dataset_id, ingested_at DESC);
CREATE INDEX IF NOT EXISTS ix_transform_run_dataset   ON integration.transform_run(dataset_id, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_relationship_source_time ON integration.relationship(source_id, ingested_at DESC);

CREATE INDEX IF NOT EXISTS gin_raw_event_payload ON integration.raw_event USING GIN (payload);
CREATE INDEX IF NOT EXISTS gin_unified_row_data  ON integration.unified_row USING GIN (data);

-- Helpful uniqueness and integrity
CREATE UNIQUE INDEX IF NOT EXISTS uq_dataset_field_name ON integration.dataset_field(dataset_id, name);

--     TRUNCATE TABLE integration.dataset CASCADE ;
--     TRUNCATE TABLE integration.ingestion_run CASCADE;
--     TRUNCATE TABLE integration.raw_event CASCADE;
--     TRUNCATE TABLE integration.dataset_field CASCADE;
--     TRUNCATE TABLE integration.dataset_mapping CASCADE;
--     TRUNCATE TABLE integration.transform_run CASCADE;
--     TRUNCATE TABLE integration.unified_row CASCADE;
--     TRUNCATE TABLE integration.metadata CASCADE;
--     TRUNCATE TABLE integration.connection CASCADE;
--     TRUNCATE TABLE integration.relationship CASCADE;
--     TRUNCATE Table integration.source CASCADE ;
--
-- DROP TABLE integration.dataset CASCADE ;
-- DROP TABLE integration.ingestion_run CASCADE;
-- DROP TABLE integration.raw_event CASCADE;
-- DROP TABLE integration.dataset_field CASCADE;
-- DROP TABLE integration.dataset_mapping CASCADE;
-- DROP TABLE integration.transform_run CASCADE;
-- DROP TABLE integration.unified_row CASCADE;
-- DROP TABLE integration.metadata CASCADE;
-- DROP TABLE integration.connection CASCADE;
-- DROP TABLE integration.relationship CASCADE;
