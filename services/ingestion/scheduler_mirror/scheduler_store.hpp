#pragma once

#include <memory>
#include <string>
#include <vector>

#include "postgres_client.hpp"
#include "scheduler-sync-v2.pb.h"

namespace ifex::offboard {

/// PostgreSQL store for scheduler sync data
///
/// Implements bidirectional state sync between cloud and vehicle:
/// - Receives V2C_SyncMessage from vehicle, updates database
/// - Provides pending cloud jobs as JobRecord for C2V_SyncMessage
class SchedulerStore {
public:
    explicit SchedulerStore(std::shared_ptr<PostgresClient> db);

    /// Ensure vehicle exists in database with optional enrichment
    void upsert_vehicle(const std::string& vehicle_id,
                        const std::string& fleet_id = "",
                        const std::string& region = "");

    /// Process a V2C sync message (vehicle → cloud)
    /// Updates jobs table with vehicle's current state
    /// Returns true if processed successfully
    bool process_v2_sync_message(
        const std::string& vehicle_id,
        const std::string& fleet_id,
        const std::string& region,
        const swdv::scheduler_sync_v2::V2C_SyncMessage& msg);

    /// Get pending cloud jobs for a vehicle (for C2V sync)
    /// Returns jobs with origin='cloud' and sync_state='pending'
    std::vector<swdv::scheduler_sync_v2::JobRecord> get_pending_cloud_jobs(
        const std::string& vehicle_id);

    /// Mark jobs as synced after vehicle confirms receipt
    void mark_jobs_synced(const std::string& vehicle_id,
                          const std::vector<std::string>& job_ids);

    /// Update sync state tracking
    void update_sync_state(
        const std::string& vehicle_id,
        uint64_t sequence,
        uint32_t checksum);

    /// Get last processed sequence for vehicle
    uint64_t get_last_sequence(const std::string& vehicle_id);

    // =========================================================================
    // Quiescence Detection (Scheduler Sync Protocol v2.4 Section 5.6)
    // =========================================================================

    /// Compute checksum of cloud's current job state for a vehicle
    /// Hash includes: job_id, authority, version, deleted, content fields
    /// Excludes: metadata (created_at_ms, updated_at_ms), execution state (status)
    uint64_t compute_cloud_state_checksum(const std::string& vehicle_id);

    /// Store the checksum from a received V2C message
    void store_v2c_checksum(const std::string& vehicle_id, uint64_t v2c_checksum);

    /// Get the last V2C checksum we received from the vehicle
    uint64_t get_last_v2c_checksum(const std::string& vehicle_id);

    /// Check if sync is quiescent for a vehicle
    /// Quiescent when: vehicle's last V2C checksum == our cloud state checksum
    /// When quiescent, no C2V messages need to be sent
    bool is_quiescent(const std::string& vehicle_id);

    /// Get quiescence state info for building C2V message
    struct QuiescenceState {
        uint64_t cloud_checksum;      // Our current state checksum
        uint64_t last_v2c_checksum;   // Last checksum we received from vehicle
        bool is_quiescent;            // True if no sync needed
    };
    QuiescenceState get_quiescence_state(const std::string& vehicle_id);

private:
    std::shared_ptr<PostgresClient> db_;

    /// Convert job status enum to string
    static std::string job_status_to_string(
        swdv::scheduler_sync_v2::JobStatus status);

    /// Convert string to job status enum
    static swdv::scheduler_sync_v2::JobStatus string_to_job_status(
        const std::string& status);

    /// Handle a V2C JobRecord - upsert into jobs table
    void handle_v2_job_record(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_v2::JobRecord& job);

    /// Handle a V2C tombstone - delete job from database
    void handle_v2_tombstone(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_v2::JobRecord& job);

    /// Handle a V2C ExecutionRecord - insert into job_executions table
    void handle_v2_execution_record(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_v2::ExecutionRecord& exec);
};

}  // namespace ifex::offboard
