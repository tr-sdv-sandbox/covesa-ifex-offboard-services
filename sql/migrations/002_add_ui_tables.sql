-- Migration: Add service_schemas and offboard_calendar tables
-- Run this on existing databases to add UI support tables

-- =============================================================================
-- Service schemas (cached IFEX schemas for UI form generation)
-- =============================================================================
CREATE TABLE IF NOT EXISTS service_schemas (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(128) NOT NULL,
    version VARCHAR(32),

    -- Full IFEX schema (JSON)
    schema_json JSONB NOT NULL,

    -- Extracted for quick lookup
    methods JSONB,           -- Array of method definitions
    struct_definitions JSONB,
    enum_definitions JSONB,

    -- Metadata
    source VARCHAR(32),      -- 'vehicle_sync' or 'manual'
    vehicle_id VARCHAR(64),  -- Source vehicle (if from sync)
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(service_name, version)
);

COMMENT ON TABLE service_schemas IS 'Cached IFEX schemas for UI dynamic form generation';

CREATE INDEX IF NOT EXISTS idx_service_schemas_name ON service_schemas(service_name);

-- =============================================================================
-- Offboard calendar (cloud-side scheduled jobs, pre-sync to vehicle)
-- =============================================================================
CREATE TABLE IF NOT EXISTS offboard_calendar (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    job_id VARCHAR(64) UNIQUE NOT NULL,

    -- Job definition
    title VARCHAR(255),
    service_name VARCHAR(128) NOT NULL,
    method_name VARCHAR(128) NOT NULL,
    parameters JSONB DEFAULT '{}'::jsonb,

    -- Schedule
    scheduled_time TIMESTAMPTZ,
    recurrence_rule VARCHAR(255),  -- iCal RRULE
    end_time TIMESTAMPTZ,

    -- Wake/Sleep policies
    wake_policy SMALLINT DEFAULT 0,      -- 0=NO_WAKE, 1=WAKE_REQUIRED
    sleep_policy SMALLINT DEFAULT 0,     -- 0=SLEEP_NORMAL, 1=INHIBIT_UNTIL_COMPLETE
    wake_lead_time_s INTEGER DEFAULT 0,

    -- Sync status
    sync_status VARCHAR(20) DEFAULT 'pending',  -- pending, synced, failed
    synced_at TIMESTAMPTZ,
    sync_error TEXT,

    -- Metadata
    created_at TIMESTAMPTZ DEFAULT NOW(),
    created_by VARCHAR(128)  -- User/API that created
);

COMMENT ON TABLE offboard_calendar IS 'Cloud-side scheduled jobs (synced to vehicle when online)';

CREATE INDEX IF NOT EXISTS idx_offboard_calendar_vehicle ON offboard_calendar(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_offboard_calendar_sync_status ON offboard_calendar(sync_status);
CREATE INDEX IF NOT EXISTS idx_offboard_calendar_scheduled ON offboard_calendar(scheduled_time);

-- Record migration
INSERT INTO kafka_offsets (consumer_group, topic, partition, committed_offset)
VALUES ('migrations', '002_add_ui_tables', 0, 1)
ON CONFLICT DO NOTHING;
