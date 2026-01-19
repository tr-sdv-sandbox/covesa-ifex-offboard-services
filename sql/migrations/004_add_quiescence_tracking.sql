-- Migration: Add quiescence tracking columns to sync_state table
-- Implements Scheduler Sync Protocol v2.4 Section 5.6 - Quiescence Detection
--
-- Quiescence = both sides have confirmed each other's checksum
-- No messages sent when quiescent (reduces bandwidth)

-- Add columns for scheduler quiescence tracking
-- cloud_state_checksum: hash of cloud's current job state for this vehicle
-- last_seen_v2c_checksum: the checksum from the last V2C message we received
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS scheduler_cloud_checksum BIGINT;
ALTER TABLE sync_state ADD COLUMN IF NOT EXISTS scheduler_v2c_checksum BIGINT;

-- Update table comment
COMMENT ON TABLE sync_state IS 'Per-vehicle sync state tracking with quiescence detection (v2.4 protocol)';

-- Add comments for new columns
COMMENT ON COLUMN sync_state.scheduler_cloud_checksum IS 'Hash of cloud job state for this vehicle (what we compute)';
COMMENT ON COLUMN sync_state.scheduler_v2c_checksum IS 'Last checksum received from vehicle V2C message';
