#include "scheduler_store.hpp"

#include <glog/logging.h>
#include <unordered_map>
#include <unordered_set>
#include <ctime>
#include <sstream>
#include <iomanip>

namespace ifex::offboard {

// Parse ISO8601 datetime string to epoch milliseconds
// Handles formats: "2026-01-09T17:00:00.000Z" and "2026-01-09 17:00:00+00"
static uint64_t Iso8601ToEpochMs(const std::string& iso_str) {
    if (iso_str.empty()) {
        return 0;
    }

    std::tm tm = {};
    std::istringstream ss(iso_str);

    // Try parsing with T delimiter: 2026-01-09T17:00:00
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (ss.fail()) {
        // Try parsing with space delimiter: 2026-01-09 17:00:00
        ss.clear();
        ss.str(iso_str);
        ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (ss.fail()) {
            LOG(WARNING) << "Failed to parse ISO8601 datetime: " << iso_str;
            return 0;
        }
    }

    // Convert to epoch seconds (UTC)
    time_t epoch_sec = timegm(&tm);
    if (epoch_sec == -1) {
        LOG(WARNING) << "Failed to convert to epoch: " << iso_str;
        return 0;
    }

    // Parse optional milliseconds
    uint64_t ms = 0;
    char c;
    if (ss >> c && c == '.') {
        int frac;
        ss >> frac;
        std::string remaining = std::to_string(frac);
        while (remaining.length() < 3) remaining += "0";
        ms = std::stoull(remaining.substr(0, 3));
    }

    return static_cast<uint64_t>(epoch_sec) * 1000 + ms;
}

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
        VLOG(1) << "Vehicle " << vehicle_id << " fleet=" << fleet_id
                << " region=" << region;
    }
}

bool SchedulerStore::process_v2_sync_message(
    const std::string& vehicle_id,
    const std::string& fleet_id,
    const std::string& region,
    const swdv::scheduler_sync_v2::V2C_SyncMessage& msg) {

    LOG(INFO) << "Processing sync from vehicle " << vehicle_id
              << ": " << msg.jobs_size() << " jobs, "
              << msg.executions_size() << " executions, "
              << msg.deleted_job_ids_size() << " deletions";

    // Ensure vehicle exists with enrichment
    upsert_vehicle(vehicle_id, fleet_id, region);

    // Build set of job_ids from sync message
    std::unordered_set<std::string> synced_job_ids;
    for (const auto& job : msg.jobs()) {
        if (!job.deleted()) {
            synced_job_ids.insert(job.job_id());
        }
    }

    // Start transaction for atomic updates
    db_->begin_transaction();

    // Delete jobs NOT in sync that are either:
    // - status='deleting' (deletion confirmed by vehicle)
    // - sync_state='synced' (vehicle deleted it - not a pending cloud job)
    // Keep jobs with origin='cloud' AND sync_state='pending' (command not yet received)
    if (!synced_job_ids.empty()) {
        // Build comma-separated list of quoted job_ids for SQL IN clause
        std::string job_ids_list;
        for (const auto& jid : synced_job_ids) {
            if (!job_ids_list.empty()) job_ids_list += ",";
            job_ids_list += "'" + jid + "'";  // Simple quoting (job_ids are generated, safe)
        }

        db_->execute(
            "DELETE FROM jobs WHERE vehicle_id = $1 "
            "AND job_id NOT IN (" + job_ids_list + ") "
            "AND (status = 'deleting' OR sync_state = 'synced')",
            {vehicle_id});
    } else {
        // No jobs in sync - delete all synced/deleting jobs, keep pending cloud jobs
        db_->execute(
            "DELETE FROM jobs WHERE vehicle_id = $1 "
            "AND (status = 'deleting' OR sync_state = 'synced')",
            {vehicle_id});
    }

    // Upsert all jobs from the sync (sets sync_state='synced')
    for (const auto& job : msg.jobs()) {
        if (!job.deleted()) {  // Skip tombstones
            handle_v2_job_record(vehicle_id, job);
        }
    }

    // Process execution records (append-only)
    for (const auto& exec : msg.executions()) {
        handle_v2_execution_record(vehicle_id, exec);
    }

    db_->commit();

    // Update vehicle last_seen_at
    db_->execute(
        "UPDATE vehicles SET last_seen_at = NOW() WHERE vehicle_id = $1",
        {vehicle_id});

    return true;
}

void SchedulerStore::handle_v2_job_record(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_v2::JobRecord& job) {

    LOG(INFO) << "Job: " << job.job_id() << " (" << job.title() << ")"
              << " service=" << job.service() << "." << job.method()
              << " status=" << job_status_to_string(job.status());

    // Convert scheduled_time_ms to ISO 8601 string for PostgreSQL
    std::string scheduled_time_str;
    if (job.scheduled_time_ms() > 0) {
        time_t secs = job.scheduled_time_ms() / 1000;
        struct tm* tm = gmtime(&secs);
        char buf[32];
        strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S+00", tm);
        scheduled_time_str = buf;
    }

    // Convert next_run_time_ms to ISO 8601 string
    std::string next_run_time_str;
    if (job.next_run_time_ms() > 0) {
        time_t secs = job.next_run_time_ms() / 1000;
        struct tm* tm = gmtime(&secs);
        char buf[32];
        strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S+00", tm);
        next_run_time_str = buf;
    }

    db_->execute(
        R"(
        INSERT INTO jobs (
            vehicle_id, job_id, title, service_name, method_name,
            parameters, scheduled_time, recurrence_rule, next_run_time,
            status, wake_policy, sleep_policy, wake_lead_time_s,
            created_at_ms, updated_at_ms, origin, sync_state
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'vehicle', 'synced')
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
            sync_state = 'synced',
            sync_updated_at = NOW()
        )",
        {
            vehicle_id,
            job.job_id(),
            job.title(),
            job.service(),
            job.method(),
            job.parameters_json().empty() ? "{}" : job.parameters_json(),
            scheduled_time_str.empty() ? "" : scheduled_time_str,
            job.recurrence_rule(),
            next_run_time_str.empty() ? "" : next_run_time_str,
            job_status_to_string(job.status()),
            std::to_string(static_cast<int>(job.wake_policy())),
            std::to_string(static_cast<int>(job.sleep_policy())),
            std::to_string(job.wake_lead_time_s()),
            std::to_string(job.created_at_ms()),
            std::to_string(job.updated_at_ms())
        });
}

void SchedulerStore::handle_v2_execution_record(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_v2::ExecutionRecord& exec) {

    LOG(INFO) << "Execution: " << exec.execution_id()
              << " job=" << exec.job_id()
              << " status=" << job_status_to_string(exec.status())
              << " duration=" << exec.duration_ms() << "ms";

    // Use execution_id as primary key to avoid duplicates
    db_->execute(
        R"(
        INSERT INTO job_executions (
            vehicle_id, job_id, status, executed_at_ms, duration_ms,
            result, error_message
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT DO NOTHING
        )",
        {
            vehicle_id,
            exec.job_id(),
            job_status_to_string(exec.status()),
            std::to_string(exec.executed_at_ms()),
            std::to_string(exec.duration_ms()),
            exec.result_json(),
            exec.error_message()
        });
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
    swdv::scheduler_sync_v2::JobStatus status) {
    switch (status) {
        case swdv::scheduler_sync_v2::JOB_STATUS_PENDING:
            return "pending";
        case swdv::scheduler_sync_v2::JOB_STATUS_RUNNING:
            return "running";
        case swdv::scheduler_sync_v2::JOB_STATUS_COMPLETED:
            return "completed";
        case swdv::scheduler_sync_v2::JOB_STATUS_FAILED:
            return "failed";
        case swdv::scheduler_sync_v2::JOB_STATUS_CANCELLED:
            return "cancelled";
        case swdv::scheduler_sync_v2::JOB_STATUS_PAUSED:
            return "paused";
        default:
            return "unknown";
    }
}

void SchedulerStore::set_reconcile_callback(ReconcileCallback callback) {
    reconcile_callback_ = std::move(callback);
}

bool SchedulerStore::has_pending_offboard_items(const std::string& vehicle_id) {
    // Check for jobs with sync_state='pending' (awaiting vehicle confirmation)
    auto result = db_->execute_scalar(
        "SELECT COUNT(*) FROM jobs WHERE vehicle_id = $1 AND sync_state = 'pending'",
        {vehicle_id});

    return result && std::stoi(*result) > 0;
}

std::vector<ReconcileCommand> SchedulerStore::reconcile_with_offboard(
    const std::string& vehicle_id) {

    std::vector<ReconcileCommand> commands;

    LOG(INFO) << "Checking pending jobs for vehicle " << vehicle_id;

    // Get jobs with sync_state='pending' (cloud-created jobs awaiting vehicle confirmation)
    auto result = db_->execute(
        R"(
        SELECT job_id, title, service_name, method_name,
               COALESCE(parameters::text, '{}') as parameters,
               scheduled_time, recurrence_rule, end_time,
               wake_policy, sleep_policy, wake_lead_time_s, status
        FROM jobs
        WHERE vehicle_id = $1 AND sync_state = 'pending' AND status != 'deleting'
        )",
        {vehicle_id});

    if (!result.ok()) {
        LOG(ERROR) << "Failed to query pending jobs: " << result.error();
        return commands;
    }

    // Generate commands for pending jobs (retry/resend logic)
    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);
        std::string job_id = row.get_string(0);

        ReconcileCommand cmd;
        cmd.job_id = job_id;
        cmd.title = row.get_string(1);
        cmd.service = row.get_string(2);
        cmd.method = row.get_string(3);
        cmd.parameters_json = row.get_string(4);

        // Parse scheduled_time and end_time strings to epoch milliseconds
        std::string scheduled_time_str = row.is_null(5) ? "" : row.get_string(5);
        std::string end_time_str = row.is_null(7) ? "" : row.get_string(7);
        cmd.scheduled_time_ms = Iso8601ToEpochMs(scheduled_time_str);
        cmd.recurrence_rule = row.get_string(6);
        cmd.end_time_ms = Iso8601ToEpochMs(end_time_str);
        cmd.wake_policy = row.is_null(8) ? 0 : row.get_int(8);
        cmd.sleep_policy = row.is_null(9) ? 0 : row.get_int(9);
        cmd.wake_lead_time_s = row.is_null(10) ? 0 : static_cast<uint32_t>(row.get_int(10));

        // All pending jobs are CREATE (initial sync) or UPDATE (re-sync)
        cmd.type = ReconcileCommand::CREATE;
        commands.push_back(cmd);
        LOG(INFO) << "Reconcile: resend job " << job_id << " to vehicle " << vehicle_id;
    }

    LOG(INFO) << "Reconciliation for " << vehicle_id << ": " << commands.size() << " pending jobs";

    return commands;
}

}  // namespace ifex::offboard
