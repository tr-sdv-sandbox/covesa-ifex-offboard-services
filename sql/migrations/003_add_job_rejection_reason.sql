-- Migration 003: Add rejection_reason column to jobs table
-- Supports SYNC_STATE_REJECTED for jobs that vehicle refuses to create
-- (e.g., invalid service/method, permission denied)

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS rejection_reason TEXT;

COMMENT ON COLUMN jobs.rejection_reason IS 'Why vehicle rejected job (when sync_state=rejected)';
