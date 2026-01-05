#include "scheduler_store.hpp"

#include <glog/logging.h>

#include "scheduler_codec.hpp"

namespace ifex::offboard {

SchedulerStore::SchedulerStore(std::shared_ptr<PostgresClient> db)
    : db_(std::move(db)) {}

void SchedulerStore::upsert_vehicle(const std::string& vehicle_id,
                                     const std::string& fleet_id,
                                     const std::string& region) {
    // Use upsert_vehicle function
    db_->execute(
        "SELECT upsert_vehicle($1)",
        {vehicle_id});

    // Update fleet_id and region if provided
    if (!fleet_id.empty() || !region.empty()) {
        // TODO: Add fleet_id and region columns to vehicles table
        // For now, just log the enrichment data
        VLOG(1) << "Vehicle " << vehicle_id << " fleet=" << fleet_id
                << " region=" << region;
    }
}

void SchedulerStore::process_offboard_message(
    const scheduler::scheduler_offboard_t& msg) {

    // Extract vehicle_id from offboard metadata (verified by ACL)
    const std::string& vehicle_id = msg.metadata().vehicle_id();
    const std::string& fleet_id = msg.metadata().fleet_id();
    const std::string& region = msg.metadata().region();

    // Get the embedded sync message
    const auto& sync_msg = msg.sync_message();

    // Ensure vehicle exists with enrichment
    upsert_vehicle(vehicle_id, fleet_id, region);

    // Process each event
    for (const auto& event : sync_msg.events()) {
        LOG(INFO) << "Processing " << scheduler_event_type_name(event.event_type())
                  << " for vehicle " << vehicle_id;

        switch (event.event_type()) {
            case swdv::scheduler_sync_envelope::FULL_SYNC:
                handle_full_sync(vehicle_id, sync_msg);
                break;

            case swdv::scheduler_sync_envelope::JOB_CREATED:
                if (event.has_job_info()) {
                    handle_job_created(vehicle_id, event.job_info());
                }
                break;

            case swdv::scheduler_sync_envelope::JOB_UPDATED:
                if (event.has_job_info()) {
                    handle_job_updated(vehicle_id, event.job_info());
                }
                break;

            case swdv::scheduler_sync_envelope::JOB_DELETED:
                handle_job_deleted(vehicle_id, event.job_id());
                break;

            case swdv::scheduler_sync_envelope::JOB_EXECUTED:
                if (event.has_execution_result()) {
                    handle_job_executed(vehicle_id, event.execution_result());
                }
                break;

            case swdv::scheduler_sync_envelope::HEARTBEAT:
                handle_heartbeat(vehicle_id, event.timestamp_ns());
                break;
        }
    }

    // Update sync state
    if (!sync_msg.events().empty()) {
        uint64_t last_seq = sync_msg.events().rbegin()->sequence_number();
        update_sync_state(vehicle_id, last_seq, sync_msg.state_checksum());
    }
}

void SchedulerStore::handle_full_sync(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::sync_message_t& msg) {

    LOG(INFO) << "Full sync for vehicle " << vehicle_id
              << " with " << msg.active_jobs_count() << " active jobs";

    // Start transaction
    db_->begin_transaction();

    // Delete all existing jobs for this vehicle
    db_->execute(
        "DELETE FROM jobs WHERE vehicle_id = $1",
        {vehicle_id});

    // Insert all jobs from the sync
    for (const auto& event : msg.events()) {
        if (event.event_type() == swdv::scheduler_sync_envelope::FULL_SYNC &&
            event.has_job_info()) {
            handle_job_created(vehicle_id, event.job_info());
        }
    }

    db_->commit();
}

void SchedulerStore::handle_job_created(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::job_info_t& info) {

    LOG(INFO) << "Creating job: " << job_info_to_string(info);

    db_->execute(
        R"(
        INSERT INTO jobs (
            vehicle_id, job_id, title, service_name, method_name,
            parameters, scheduled_time, recurrence_rule, next_run_time,
            status, wake_policy, sleep_policy, wake_lead_time_s,
            created_at_ms, updated_at_ms
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11, $12, $13, $14, $15)
        ON CONFLICT (vehicle_id, job_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            service_name = EXCLUDED.service_name,
            method_name = EXCLUDED.method_name,
            parameters = EXCLUDED.parameters,
            scheduled_time = EXCLUDED.scheduled_time,
            recurrence_rule = EXCLUDED.recurrence_rule,
            next_run_time = EXCLUDED.next_run_time,
            status = EXCLUDED.status,
            wake_policy = EXCLUDED.wake_policy,
            sleep_policy = EXCLUDED.sleep_policy,
            wake_lead_time_s = EXCLUDED.wake_lead_time_s,
            created_at_ms = EXCLUDED.created_at_ms,
            updated_at_ms = EXCLUDED.updated_at_ms,
            sync_updated_at = NOW()
        )",
        {
            vehicle_id,
            info.job_id(),
            info.title(),
            info.service(),
            info.method(),
            info.parameters().empty() ? "{}" : info.parameters(),
            info.scheduled_time(),
            info.recurrence_rule(),
            info.next_run_time(),
            job_status_to_string(info.status()),
            std::to_string(static_cast<int>(info.wake_policy())),
            std::to_string(static_cast<int>(info.sleep_policy())),
            std::to_string(info.wake_lead_time_s()),
            std::to_string(info.created_at_ms()),
            std::to_string(info.updated_at_ms())
        });
}

void SchedulerStore::handle_job_updated(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::job_info_t& info) {

    LOG(INFO) << "Updating job: " << info.job_id();

    db_->execute(
        R"(
        UPDATE jobs SET
            title = $3,
            service_name = $4,
            method_name = $5,
            parameters = $6::jsonb,
            scheduled_time = $7,
            recurrence_rule = $8,
            next_run_time = $9,
            status = $10,
            wake_policy = $11,
            sleep_policy = $12,
            wake_lead_time_s = $13,
            updated_at_ms = $14,
            sync_updated_at = NOW()
        WHERE vehicle_id = $1 AND job_id = $2
        )",
        {
            vehicle_id,
            info.job_id(),
            info.title(),
            info.service(),
            info.method(),
            info.parameters().empty() ? "{}" : info.parameters(),
            info.scheduled_time(),
            info.recurrence_rule(),
            info.next_run_time(),
            job_status_to_string(info.status()),
            std::to_string(static_cast<int>(info.wake_policy())),
            std::to_string(static_cast<int>(info.sleep_policy())),
            std::to_string(info.wake_lead_time_s()),
            std::to_string(info.updated_at_ms())
        });
}

void SchedulerStore::handle_job_deleted(
    const std::string& vehicle_id,
    const std::string& job_id) {

    LOG(INFO) << "Deleting job: " << job_id << " from vehicle " << vehicle_id;

    db_->execute(
        "DELETE FROM jobs WHERE vehicle_id = $1 AND job_id = $2",
        {vehicle_id, job_id});
}

void SchedulerStore::handle_job_executed(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::execution_result_t& result) {

    LOG(INFO) << "Job executed: " << result.job_id()
              << " status=" << job_status_to_string(result.status())
              << " duration=" << result.duration_ms() << "ms";

    // Insert execution result
    db_->execute(
        R"(
        INSERT INTO job_executions (
            vehicle_id, job_id, status, executed_at_ms, duration_ms,
            result, error_message, next_run_time
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        )",
        {
            vehicle_id,
            result.job_id(),
            job_status_to_string(result.status()),
            std::to_string(result.executed_at_ms()),
            std::to_string(result.duration_ms()),
            result.result(),
            result.error_message(),
            result.next_run_time()
        });

    // Update job's next_run_time if provided
    if (!result.next_run_time().empty()) {
        db_->execute(
            R"(
            UPDATE jobs SET
                next_run_time = $3,
                sync_updated_at = NOW()
            WHERE vehicle_id = $1 AND job_id = $2
            )",
            {vehicle_id, result.job_id(), result.next_run_time()});
    }
}

void SchedulerStore::handle_heartbeat(
    const std::string& vehicle_id,
    uint64_t timestamp_ns) {

    VLOG(1) << "Scheduler heartbeat from vehicle " << vehicle_id;

    // Update vehicle last_seen_at
    db_->execute(
        "UPDATE vehicles SET last_seen_at = NOW() WHERE vehicle_id = $1",
        {vehicle_id});
}

void SchedulerStore::update_sync_state(
    const std::string& vehicle_id,
    uint64_t sequence,
    uint32_t checksum) {

    db_->execute(
        "SELECT update_sync_state($1, 'scheduler', $2, $3)",
        {vehicle_id, std::to_string(sequence), std::to_string(checksum)});
}

uint64_t SchedulerStore::get_last_sequence(const std::string& vehicle_id) {
    auto result = db_->execute_scalar(
        "SELECT scheduler_sequence FROM sync_state WHERE vehicle_id = $1",
        {vehicle_id});

    if (result) {
        return std::stoull(*result);
    }
    return 0;
}

std::string SchedulerStore::job_status_to_string(
    swdv::scheduler_sync_envelope::job_sync_status_t status) {
    switch (status) {
        case swdv::scheduler_sync_envelope::PENDING:
            return "pending";
        case swdv::scheduler_sync_envelope::RUNNING:
            return "running";
        case swdv::scheduler_sync_envelope::COMPLETED:
            return "completed";
        case swdv::scheduler_sync_envelope::FAILED:
            return "failed";
        case swdv::scheduler_sync_envelope::CANCELLED:
            return "cancelled";
        default:
            return "unknown";
    }
}

}  // namespace ifex::offboard
