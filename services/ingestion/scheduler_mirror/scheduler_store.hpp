#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>

#include "postgres_client.hpp"
#include "scheduler-command-envelope.pb.h"
#include "scheduler-sync-v2.pb.h"

namespace ifex::offboard {

/// Reconciliation command to send to vehicle
struct ReconcileCommand {
    enum Type { CREATE, UPDATE, DELETE };
    Type type;
    std::string job_id;
    std::string title;
    std::string service;
    std::string method;
    std::string parameters_json;
    uint64_t scheduled_time_ms = 0;  // Epoch milliseconds UTC
    std::string recurrence_rule;
    uint64_t end_time_ms = 0;        // Epoch milliseconds UTC (0 = no end)
    int wake_policy = 0;
    int sleep_policy = 0;
    uint32_t wake_lead_time_s = 0;
};

/// Callback for sending reconciliation commands
using ReconcileCallback = std::function<void(const std::string& vehicle_id,
                                              const std::vector<ReconcileCommand>& commands)>;

/// PostgreSQL store for scheduler sync data
class SchedulerStore {
public:
    explicit SchedulerStore(std::shared_ptr<PostgresClient> db);

    /// Ensure vehicle exists in database with optional enrichment
    void upsert_vehicle(const std::string& vehicle_id,
                        const std::string& fleet_id = "",
                        const std::string& region = "");

    /// Process a v2 sync message (from Kafka)
    /// Returns true if processed successfully, false if parse failed
    bool process_v2_sync_message(
        const std::string& vehicle_id,
        const std::string& fleet_id,
        const std::string& region,
        const swdv::scheduler_sync_v2::V2C_SyncMessage& msg);

    /// Update sync state
    void update_sync_state(
        const std::string& vehicle_id,
        uint64_t sequence,
        uint32_t checksum);

    /// Get last processed sequence for vehicle
    uint64_t get_last_sequence(const std::string& vehicle_id);

    /// Set callback for reconciliation commands
    void set_reconcile_callback(ReconcileCallback callback);

    /// Reconcile offboard_calendar with vehicle state
    /// Returns list of commands needed to align vehicle with cloud
    /// Called after processing vehicle sync to push pending cloud changes
    std::vector<ReconcileCommand> reconcile_with_offboard(const std::string& vehicle_id);

    /// Check if vehicle has pending offboard calendar items
    bool has_pending_offboard_items(const std::string& vehicle_id);

private:
    std::shared_ptr<PostgresClient> db_;
    ReconcileCallback reconcile_callback_;

    /// Convert job status enum to string
    static std::string job_status_to_string(
        swdv::scheduler_sync_v2::JobStatus status);

    /// Handle a v2 JobRecord - upsert into jobs table
    void handle_v2_job_record(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_v2::JobRecord& job);

    /// Handle a v2 ExecutionRecord - insert into job_executions table
    void handle_v2_execution_record(
        const std::string& vehicle_id,
        const swdv::scheduler_sync_v2::ExecutionRecord& exec);
};

}  // namespace ifex::offboard
