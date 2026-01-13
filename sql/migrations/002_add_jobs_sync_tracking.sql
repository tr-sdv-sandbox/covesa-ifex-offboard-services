-- Migration: Add sync tracking columns to jobs table
-- This supports bidirectional job creation (cloud and vehicle)

-- Add new columns to jobs table
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS origin VARCHAR(16) DEFAULT 'vehicle';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS sync_state VARCHAR(16) DEFAULT 'synced';
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS created_by VARCHAR(128);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS end_time VARCHAR(64);

-- Add indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_jobs_sync_state ON jobs(sync_state);
CREATE INDEX IF NOT EXISTS idx_jobs_origin ON jobs(origin);

-- Update table comment
COMMENT ON TABLE jobs IS 'Scheduled jobs (cloud-created and vehicle-synced). origin tracks source, sync_state tracks confirmation.';

-- Mark existing jobs as synced (they came from vehicle sync)
UPDATE jobs SET origin = 'vehicle', sync_state = 'synced' WHERE origin IS NULL;

-- Drop deprecated offboard_calendar table
DROP TABLE IF EXISTS offboard_calendar CASCADE;
