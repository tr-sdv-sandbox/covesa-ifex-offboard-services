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
    connection_status VARCHAR(20) DEFAULT 'unknown',
    metadata JSONB DEFAULT '{}'::jsonb
);

COMMENT ON TABLE vehicles IS 'Tracked vehicles from discovery sync';

-- =============================================================================
-- Services registry (from content_id=201)
-- =============================================================================
CREATE TABLE services (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    registration_id VARCHAR(64) NOT NULL,
    service_name VARCHAR(128) NOT NULL,
    version VARCHAR(32),
    description TEXT,
    endpoint_address VARCHAR(256),
    transport_type VARCHAR(32),
    status VARCHAR(32) DEFAULT 'unknown',
    last_heartbeat_ms BIGINT,
    namespaces JSONB DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(vehicle_id, registration_id)
);

COMMENT ON TABLE services IS 'Service registry synced from vehicles (content_id=201)';

CREATE INDEX idx_services_vehicle ON services(vehicle_id);
CREATE INDEX idx_services_name ON services(service_name);
CREATE INDEX idx_services_status ON services(status);

-- =============================================================================
-- Jobs registry (from content_id=202)
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
    status VARCHAR(32) DEFAULT 'pending',
    created_at_ms BIGINT,
    updated_at_ms BIGINT,
    sync_created_at TIMESTAMPTZ DEFAULT NOW(),
    sync_updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(vehicle_id, job_id)
);

COMMENT ON TABLE jobs IS 'Scheduled jobs synced from vehicles (content_id=202)';

CREATE INDEX idx_jobs_vehicle ON jobs(vehicle_id);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_service ON jobs(service_name);

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
-- RPC request tracking (for content_id=200)
-- =============================================================================
CREATE TABLE rpc_requests (
    id SERIAL PRIMARY KEY,
    correlation_id VARCHAR(64) UNIQUE NOT NULL,
    vehicle_id VARCHAR(64) NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    service_name VARCHAR(128),
    method_name VARCHAR(128),
    parameters_json TEXT,
    timeout_ms INTEGER,
    request_timestamp_ns BIGINT,
    response_status VARCHAR(32),
    result_json TEXT,
    error_message TEXT,
    duration_ms INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    responded_at TIMESTAMPTZ
);

COMMENT ON TABLE rpc_requests IS 'Cloud-to-vehicle RPC request/response tracking';

CREATE INDEX idx_rpc_requests_correlation ON rpc_requests(correlation_id);
CREATE INDEX idx_rpc_requests_vehicle ON rpc_requests(vehicle_id);
CREATE INDEX idx_rpc_requests_pending ON rpc_requests(responded_at) WHERE responded_at IS NULL;

-- =============================================================================
-- Sync state tracking
-- =============================================================================
CREATE TABLE sync_state (
    vehicle_id VARCHAR(64) PRIMARY KEY REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    discovery_sequence BIGINT DEFAULT 0,
    discovery_checksum INTEGER,
    discovery_last_sync TIMESTAMPTZ,
    scheduler_sequence BIGINT DEFAULT 0,
    scheduler_checksum INTEGER,
    scheduler_last_sync TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE sync_state IS 'Per-vehicle sync sequence and checksum tracking';

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
CREATE OR REPLACE FUNCTION update_sync_state(
    p_vehicle_id VARCHAR,
    p_sync_type VARCHAR,
    p_sequence BIGINT,
    p_checksum INTEGER
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
