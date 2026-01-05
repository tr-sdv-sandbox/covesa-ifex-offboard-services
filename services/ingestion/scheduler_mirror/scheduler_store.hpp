#pragma once

#include <memory>
#include <string>

#include "postgres_client.hpp"
#include "scheduler-offboard.pb.h"
#include "scheduler-sync-envelope.pb.h"

namespace ifex::offboard {

/// PostgreSQL store for scheduler sync data
class SchedulerStore {
public:
    explicit SchedulerStore(std::shared_ptr<PostgresClient> db);

    /// Ensure vehicle exists in database with optional enrichment
    void upsert_vehicle(const std::string& vehicle_id,
                        const std::string& fleet_id = "",
                        const std::string& region = "");

    /// Process an offboard scheduler message (from Kafka)
    void process_offboard_message(
        const scheduler::scheduler_offboard_t& msg);

    /// Handle FULL_SYNC event - replace all jobs for vehicle
    void handle_full_sync(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_envelope::sync_message_t& msg);

    /// Handle JOB_CREATED event
    void handle_job_created(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_envelope::job_info_t& info);

    /// Handle JOB_UPDATED event
    void handle_job_updated(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_envelope::job_info_t& info);

    /// Handle JOB_DELETED event
    void handle_job_deleted(
        const std::string& vehicle_id,
        const std::string& job_id);

    /// Handle JOB_EXECUTED event
    void handle_job_executed(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_envelope::execution_result_t& result);

    /// Handle HEARTBEAT event
    void handle_heartbeat(
        const std::string& vehicle_id,
        uint64_t timestamp_ns);

    /// Update sync state
    void update_sync_state(
        const std::string& vehicle_id,
        uint64_t sequence,
        uint32_t checksum);

    /// Get last processed sequence for vehicle
    uint64_t get_last_sequence(const std::string& vehicle_id);

private:
    std::shared_ptr<PostgresClient> db_;

    /// Convert job status enum to string
    static std::string job_status_to_string(
        swdv::scheduler_sync_envelope::job_sync_status_t status);
};

}  // namespace ifex::offboard
