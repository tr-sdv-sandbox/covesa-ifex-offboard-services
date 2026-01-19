-- IFEX Offboard Services PostgreSQL Schema
-- This schema stores synchronized state from vehicle IFEX services

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- Vehicles table (populated from discovery sync)
-- =============================================================================
CREATE TABLE vehicles (
    vehicle_id VARCHAR(64) PRIMARY KEY,
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
    is_online BOOLEAN DEFAULT false,
    metadata JSONB DEFAULT '{}'::jsonb
);

COMMENT ON TABLE vehicles IS 'Tracked vehicles from discovery sync';

-- =============================================================================
-- Vehicle enrichment (source of truth, published to Kafka on startup/change)
-- =============================================================================
CREATE TABLE vehicle_enrichment (
    vehicle_id VARCHAR(64) PRIMARY KEY REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    fleet_id VARCHAR(64),
    region VARCHAR(64),
    model VARCHAR(128),
    year INTEGER,
    owner VARCHAR(256),
    tags JSONB DEFAULT '[]'::jsonb,
    custom_attributes JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE vehicle_enrichment IS 'Vehicle metadata (source of truth). Synced to Kafka on startup and changes polled.';

CREATE INDEX idx_vehicle_enrichment_fleet ON vehicle_enrichment(fleet_id);
CREATE INDEX idx_vehicle_enrichment_region ON vehicle_enrichment(region);

-- =============================================================================
-- Jobs registry (unified: cloud-created + vehicle-created)
-- =============================================================================
CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    job_id VARCHAR(64) NOT NULL,
    title VARCHAR(256),
    service_name VARCHAR(128),
    method_name VARCHAR(128),
    parameters JSONB DEFAULT '{}'::jsonb,
    scheduled_time VARCHAR(64),
    recurrence_rule VARCHAR(128),
    next_run_time VARCHAR(64),
    end_time VARCHAR(64),
    status VARCHAR(32) DEFAULT 'pending',
    wake_policy SMALLINT DEFAULT 0,      -- 0=NO_WAKE, 1=WAKE_REQUIRED
    sleep_policy SMALLINT DEFAULT 0,     -- 0=SLEEP_NORMAL, 1=INHIBIT_UNTIL_COMPLETE
    wake_lead_time_s INTEGER DEFAULT 0,  -- seconds before scheduled_time to wake
    created_at_ms BIGINT,
    updated_at_ms BIGINT,
    -- Sync tracking
    origin VARCHAR(16) DEFAULT 'vehicle',     -- 'cloud' or 'vehicle'
    sync_state VARCHAR(16) DEFAULT 'synced',  -- 'pending', 'synced', 'rejected'
    rejection_reason TEXT,                    -- Why vehicle rejected (when sync_state='rejected')
    created_by VARCHAR(128),                  -- User/API that created (for cloud-created)
    sync_created_at TIMESTAMPTZ DEFAULT NOW(),
    sync_updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(vehicle_id, job_id)
);

COMMENT ON TABLE jobs IS 'Scheduled jobs (cloud-created and vehicle-synced). origin tracks source, sync_state tracks confirmation.';

CREATE INDEX idx_jobs_vehicle ON jobs(vehicle_id);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_service ON jobs(service_name);
CREATE INDEX idx_jobs_sync_state ON jobs(sync_state);
CREATE INDEX idx_jobs_origin ON jobs(origin);

-- =============================================================================
-- Job execution history
-- =============================================================================
CREATE TABLE job_executions (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    job_id VARCHAR(64) NOT NULL,
    status VARCHAR(32),
    executed_at_ms BIGINT,
    duration_ms INTEGER,
    result TEXT,
    error_message TEXT,
    next_run_time VARCHAR(64),
    received_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE job_executions IS 'Job execution results synced from vehicles';

CREATE INDEX idx_job_executions_vehicle ON job_executions(vehicle_id);
CREATE INDEX idx_job_executions_job ON job_executions(job_id);
CREATE INDEX idx_job_executions_time ON job_executions(executed_at_ms DESC);

-- =============================================================================
-- RPC request tracking - REMOVED
-- RPCs are now ephemeral (in-memory only in dispatcher_api)
-- No database persistence needed for RPC correlation
-- =============================================================================

-- =============================================================================
-- Sync state tracking (with quiescence detection per v2.4 protocol)
-- =============================================================================
CREATE TABLE sync_state (
    vehicle_id VARCHAR(64) PRIMARY KEY REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    discovery_sequence BIGINT DEFAULT 0,
    discovery_checksum BIGINT,  -- uint32_t can exceed INT_MAX
    discovery_last_sync TIMESTAMPTZ,
    scheduler_sequence BIGINT DEFAULT 0,
    scheduler_checksum BIGINT,  -- uint32_t can exceed INT_MAX
    scheduler_last_sync TIMESTAMPTZ,
    -- Quiescence detection (Scheduler Sync Protocol v2.4 Section 5.6)
    scheduler_cloud_checksum BIGINT,  -- Hash of cloud's current job state
    scheduler_v2c_checksum BIGINT,    -- Last checksum received from vehicle V2C
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE sync_state IS 'Per-vehicle sync state tracking with quiescence detection (v2.4 protocol)';
COMMENT ON COLUMN sync_state.scheduler_cloud_checksum IS 'Hash of cloud job state for this vehicle (what we compute)';
COMMENT ON COLUMN sync_state.scheduler_v2c_checksum IS 'Last checksum received from vehicle V2C message';

-- =============================================================================
-- Kafka offset tracking (for exactly-once processing)
-- =============================================================================
CREATE TABLE kafka_offsets (
    consumer_group VARCHAR(128) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition INTEGER NOT NULL,
    committed_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition)
);

COMMENT ON TABLE kafka_offsets IS 'Kafka consumer offset tracking for exactly-once semantics';

-- =============================================================================
-- Helper functions
-- =============================================================================

-- Function to upsert vehicle on first contact
CREATE OR REPLACE FUNCTION upsert_vehicle(p_vehicle_id VARCHAR)
RETURNS VOID AS $$
BEGIN
    INSERT INTO vehicles (vehicle_id, first_seen_at, last_seen_at)
    VALUES (p_vehicle_id, NOW(), NOW())
    ON CONFLICT (vehicle_id) DO UPDATE
    SET last_seen_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to update sync state
-- Drop old versions with different parameter types to avoid ambiguity
DROP FUNCTION IF EXISTS update_sync_state(VARCHAR, VARCHAR, BIGINT, INTEGER);
DROP FUNCTION IF EXISTS update_sync_state(VARCHAR, VARCHAR, BIGINT, BIGINT);

CREATE OR REPLACE FUNCTION update_sync_state(
    p_vehicle_id VARCHAR,
    p_sync_type VARCHAR,
    p_sequence BIGINT,
    p_checksum BIGINT  -- uint32_t can exceed INT_MAX
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO sync_state (vehicle_id, updated_at)
    VALUES (p_vehicle_id, NOW())
    ON CONFLICT (vehicle_id) DO NOTHING;

    IF p_sync_type = 'discovery' THEN
        UPDATE sync_state
        SET discovery_sequence = p_sequence,
            discovery_checksum = p_checksum,
            discovery_last_sync = NOW(),
            updated_at = NOW()
        WHERE vehicle_id = p_vehicle_id;
    ELSIF p_sync_type = 'scheduler' THEN
        UPDATE sync_state
        SET scheduler_sequence = p_sequence,
            scheduler_checksum = p_checksum,
            scheduler_last_sync = NOW(),
            updated_at = NOW()
        WHERE vehicle_id = p_vehicle_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Schema registry (hash-based deduplication across fleet)
-- =============================================================================
CREATE TABLE schema_registry (
    schema_hash VARCHAR(64) PRIMARY KEY,  -- SHA-256 hex (64 chars)
    ifex_schema TEXT NOT NULL,            -- Full IFEX YAML
    service_name VARCHAR(128) NOT NULL,   -- Extracted for convenience
    version VARCHAR(32),                  -- Extracted for convenience

    -- Pre-parsed JSONB for fast API queries
    methods JSONB,                        -- Array of method definitions
    struct_definitions JSONB,             -- Struct types
    enum_definitions JSONB,               -- Enum types

    -- Metadata
    first_seen_at TIMESTAMPTZ DEFAULT NOW(),
    first_vehicle_id VARCHAR(64)          -- Which vehicle first provided this schema
);

COMMENT ON TABLE schema_registry IS 'IFEX schemas indexed by SHA-256 hash (fleet-wide deduplication)';

CREATE INDEX idx_schema_registry_name ON schema_registry(service_name);

-- =============================================================================
-- Vehicle-to-schema junction table
-- =============================================================================
CREATE TABLE vehicle_schemas (
    vehicle_id VARCHAR(64) NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    schema_hash VARCHAR(64) NOT NULL REFERENCES schema_registry(schema_hash) ON DELETE CASCADE,
    last_seen_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (vehicle_id, schema_hash)
);

COMMENT ON TABLE vehicle_schemas IS 'Links vehicles to their service schemas';

CREATE INDEX idx_vehicle_schemas_vehicle ON vehicle_schemas(vehicle_id);
CREATE INDEX idx_vehicle_schemas_hash ON vehicle_schemas(schema_hash);

-- =============================================================================
-- Views for API queries
-- =============================================================================

-- Services by vehicle (for Fleet tab)
CREATE OR REPLACE VIEW vehicle_services_view AS
SELECT
    vs.vehicle_id,
    sr.service_name,
    sr.version,
    sr.schema_hash,
    sr.methods,
    vs.last_seen_at
FROM vehicle_schemas vs
JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash;

-- Unique services across fleet (for Services tab)
CREATE OR REPLACE VIEW fleet_services_view AS
SELECT
    sr.service_name,
    sr.version,
    sr.schema_hash,
    sr.methods,
    sr.struct_definitions,
    sr.enum_definitions,
    COUNT(DISTINCT vs.vehicle_id) AS vehicle_count,
    sr.first_seen_at
FROM schema_registry sr
LEFT JOIN vehicle_schemas vs ON sr.schema_hash = vs.schema_hash
GROUP BY sr.schema_hash, sr.service_name, sr.version, sr.methods, sr.struct_definitions, sr.enum_definitions, sr.first_seen_at
ORDER BY sr.service_name, sr.version;

