#include "scheduler_store.hpp"

#include "job.hpp"
#include "job_hash.hpp"

#include <glog/logging.h>
#include <ctime>
#include <functional>
#include <sstream>
#include <iomanip>
#include <algorithm>

namespace ifex::offboard {

namespace sched_lib = ifex::scheduler;

// Parse ISO 8601 timestamp string to epoch milliseconds
// Handles formats like: 2026-01-13T01:00:00.000Z or 2026-01-13 01:00:00
static uint64_t parse_timestamp_to_ms(const std::string& timestamp_str) {
    if (timestamp_str.empty()) {
        return 0;
    }

    std::tm tm = {};
    std::string str = timestamp_str;

    // Replace 'T' with space if present (ISO 8601)
    size_t t_pos = str.find('T');
    if (t_pos != std::string::npos) {
        str[t_pos] = ' ';
    }

    // Remove timezone suffix (.000Z or Z)
    size_t dot_pos = str.find('.');
    if (dot_pos != std::string::npos) {
        str = str.substr(0, dot_pos);
    }
    size_t z_pos = str.find('Z');
    if (z_pos != std::string::npos) {
        str = str.substr(0, z_pos);
    }

    std::istringstream ss(str);
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) {
        LOG(WARNING) << "Failed to parse timestamp: " << timestamp_str;
        return 0;
    }

    return static_cast<uint64_t>(timegm(&tm)) * 1000;
}

// Helper to create a library Job struct from parameters (for hash computation)
static sched_lib::Job make_job_for_hash(
    const std::string& job_id,
    const std::string& title,
    const std::string& service_name,
    const std::string& method_name,
    const std::string& parameters,
    uint64_t scheduled_time_ms,
    const std::string& recurrence_rule,
    uint64_t end_time_ms,
    bool paused,
    int wake_policy,
    int sleep_policy,
    uint32_t wake_lead_time_s) {
    sched_lib::Job job;
    job.job_id = job_id;
    job.title = title;
    job.service = service_name;
    job.method = method_name;
    job.parameters_json = parameters;
    job.scheduled_time_ms = scheduled_time_ms;
    job.recurrence_rule = recurrence_rule;
    job.end_time_ms = end_time_ms;
    job.paused = paused;
    job.wake_policy = static_cast<sched_lib::WakePolicy>(wake_policy);
    job.sleep_policy = static_cast<sched_lib::SleepPolicy>(sleep_policy);
    job.wake_lead_time_s = wake_lead_time_s;
    return job;
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

    // Count tombstones (jobs with deleted=true)
    int tombstone_count = 0;
    for (const auto& job : msg.jobs()) {
        if (job.deleted()) tombstone_count++;
    }

    LOG(INFO) << "Processing V2C sync from vehicle " << vehicle_id
              << ": " << msg.jobs_size() << " jobs (" << tombstone_count << " tombstones), "
              << msg.executions_size() << " executions";

    // Ensure vehicle exists with enrichment
    upsert_vehicle(vehicle_id, fleet_id, region);

    // Start transaction for atomic updates
    db_->begin_transaction();

    // NOTE: We do NOT reset cloud jobs to pending based on V2C job count!
    //
    // The vehicle sync bridge is configured with "Terminal states only: yes",
    // meaning it only sends jobs in terminal states (completed/failed/cancelled).
    // A V2C message with 0 jobs means "no terminal jobs to report", NOT "vehicle has no jobs".
    //
    // The correct sync flow is:
    // 1. Cloud creates job -> sync_state='pending'
    // 2. Cloud sends C2V with pending jobs -> vehicle receives and stores
    // 3. Vehicle echoes job back in V2C (immediate ack) -> cloud marks sync_state='synced'
    // 4. Subsequent V2C messages only contain terminal jobs or changes
    //
    // DO NOT implement "drift detection" that resets jobs based on V2C content.
    // This would cause an infinite sync loop since pending jobs are never in V2C.

    // Upsert all jobs from the sync
    // When vehicle syncs a job that was pending from cloud, mark it synced
    for (const auto& job : msg.jobs()) {
        if (job.deleted()) {
            // Handle tombstone - vehicle confirmed deletion
            handle_v2_tombstone(vehicle_id, job);
        } else {
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

    // Per Scheduler Sync Protocol v2.4:
    // 1. If incoming checksum matches stored checksum -> already applied, drop
    // 2. Compare version vectors to determine if we should accept
    // 3. Only accept if remote dominates or conflict is resolved in remote's favor

    // First, query existing job to get version vector and compute checksum
    auto existing = db_->execute(
        R"(
        SELECT cloud_seq, vehicle_seq, job_id, title, service_name, method_name,
               COALESCE(parameters::text, '{}') as parameters,
               scheduled_time, recurrence_rule, end_time,
               wake_policy, sleep_policy, wake_lead_time_s, paused, origin, sync_state
        FROM jobs
        WHERE vehicle_id = $1 AND job_id = $2
        )",
        {vehicle_id, job.job_id()});

    // Compute incoming job's checksum using library
    uint64_t incoming_checksum = sched_lib::compute_job_content_hash(make_job_for_hash(
        job.job_id(),
        job.title(),
        job.service(),
        job.method(),
        job.parameters_json().empty() ? "{}" : job.parameters_json(),
        job.scheduled_time_ms(),
        job.recurrence_rule(),
        job.end_time_ms(),
        job.paused(),
        static_cast<int>(job.wake_policy()),
        static_cast<int>(job.sleep_policy()),
        job.wake_lead_time_s()));

    if (existing.ok() && existing.num_rows() > 0) {
        auto row = existing.row(0);

        // Get existing version vector
        uint64_t local_cloud_seq = row.is_null(0) ? 0 : static_cast<uint64_t>(row.get_int64(0));
        uint64_t local_vehicle_seq = row.is_null(1) ? 0 : static_cast<uint64_t>(row.get_int64(1));

        // Compute existing job's checksum
        uint64_t scheduled_ms = 0;
        if (!row.is_null(7)) {
            scheduled_ms = parse_timestamp_to_ms(row.get_string(7));
        }
        uint64_t end_time_ms = 0;
        if (!row.is_null(9)) {
            end_time_ms = parse_timestamp_to_ms(row.get_string(9));
        }
        bool existing_paused = !row.is_null(13) && (row.get_string(13) == "t" || row.get_string(13) == "true");

        uint64_t existing_checksum = sched_lib::compute_job_content_hash(make_job_for_hash(
            row.get_string(2),   // job_id
            row.get_string(3),   // title
            row.get_string(4),   // service_name
            row.get_string(5),   // method_name
            row.get_string(6),   // parameters
            scheduled_ms,
            row.get_string(8),   // recurrence_rule
            end_time_ms,
            existing_paused,
            row.is_null(10) ? 0 : row.get_int(10),   // wake_policy
            row.is_null(11) ? 0 : row.get_int(11),   // sleep_policy
            row.is_null(12) ? 0 : static_cast<uint32_t>(row.get_int(12))  // wake_lead_time_s
        ));

        // Get sync state first - needed for both checksum match and version comparison
        std::string existing_origin = row.get_string(14);
        std::string existing_sync_state = row.get_string(15);

        // Get incoming version vector
        uint64_t remote_cloud_seq = job.version().cloud_seq();
        uint64_t remote_vehicle_seq = job.version().vehicle_seq();

        // Check 1: If checksums match, content is identical
        // But still check if this is a sync confirmation (vehicle echoing back same version)
        if (incoming_checksum == existing_checksum) {
            // Even with matching checksum, check if this confirms a pending sync
            bool versions_equal = (local_cloud_seq == remote_cloud_seq &&
                                  local_vehicle_seq == remote_vehicle_seq);
            if (versions_equal && existing_sync_state == "pending") {
                LOG(INFO) << "Job " << job.job_id() << ": vehicle confirmed receipt (checksum match, version {"
                          << local_cloud_seq << "," << local_vehicle_seq << "}) - marking synced";
                db_->execute(
                    "UPDATE jobs SET sync_state = 'synced', updated_at_ms = $1 "
                    "WHERE vehicle_id = $2 AND job_id = $3",
                    {std::to_string(job.updated_at_ms()), vehicle_id, job.job_id()});
            } else {
                VLOG(1) << "Job " << job.job_id() << ": checksum match, already applied - dropping";
            }
            return;  // No content change needed
        }

        // Check 2: Version-based conflict resolution for content changes
        LOG(INFO) << "Job " << job.job_id() << ": comparing versions - "
                  << "local={" << local_cloud_seq << "," << local_vehicle_seq << "} "
                  << "remote={" << remote_cloud_seq << "," << remote_vehicle_seq << "}";

        // Compare version vectors per Scheduler Sync Protocol v2.4:
        // - If local dominates remote: reject (our pending changes are newer)
        // - If remote dominates local: accept
        // - If conflict: use authority (AUTHORITY_CLOUD wins on cloud side)

        bool local_dominates = (local_cloud_seq >= remote_cloud_seq &&
                               local_vehicle_seq >= remote_vehicle_seq &&
                               (local_cloud_seq > remote_cloud_seq || local_vehicle_seq > remote_vehicle_seq));

        bool remote_dominates = (remote_cloud_seq >= local_cloud_seq &&
                                remote_vehicle_seq >= local_vehicle_seq &&
                                (remote_cloud_seq > local_cloud_seq || remote_vehicle_seq > local_vehicle_seq));

        // Check for version equality - vehicle is confirming receipt
        bool versions_equal = (local_cloud_seq == remote_cloud_seq &&
                              local_vehicle_seq == remote_vehicle_seq);

        if (versions_equal) {
            // Vehicle echoed back the same version - this is confirmation of receipt
            if (existing_sync_state == "pending") {
                LOG(INFO) << "Job " << job.job_id() << ": vehicle confirmed receipt (version {"
                          << local_cloud_seq << "," << local_vehicle_seq << "}) - marking synced";
                db_->execute(
                    "UPDATE jobs SET sync_state = 'synced', updated_at_ms = $1 "
                    "WHERE vehicle_id = $2 AND job_id = $3",
                    {std::to_string(job.updated_at_ms()), vehicle_id, job.job_id()});
            } else {
                VLOG(1) << "Job " << job.job_id() << ": version match, already synced";
            }
            return;  // No content change needed
        }

        if (local_dominates) {
            LOG(INFO) << "Job " << job.job_id() << ": local dominates (cloud_seq="
                      << local_cloud_seq << "/" << remote_cloud_seq
                      << ", vehicle_seq=" << local_vehicle_seq << "/" << remote_vehicle_seq
                      << ") - rejecting stale V2C";
            return;  // Our version is newer, don't overwrite
        }

        if (!remote_dominates) {
            // Conflict: neither dominates. On cloud side, cloud authority wins.
            // Since this job is being managed by cloud (origin='cloud' when sync_state='pending'),
            // we reject the stale vehicle update
            if (existing_origin == "cloud" && existing_sync_state == "pending") {
                LOG(INFO) << "Job " << job.job_id() << ": conflict with pending cloud change - cloud wins";
                return;  // Cloud authority wins the conflict
            }
            // Otherwise, accept vehicle's version (vehicle-created job or already synced)
            LOG(INFO) << "Job " << job.job_id() << ": conflict resolved in favor of vehicle";
        }
    }

    // Accept the update - either new job or remote dominates/wins conflict
    LOG(INFO) << "Job: " << job.job_id() << " (" << job.title() << ")"
              << " service=" << job.service() << "." << job.method()
              << " status=" << job_status_to_string(job.status())
              << " checksum=" << incoming_checksum;

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

    // Upsert job with version vector - vehicle is confirming it has this state
    db_->execute(
        R"(
        INSERT INTO jobs (
            vehicle_id, job_id, title, service_name, method_name,
            parameters, scheduled_time, recurrence_rule, next_run_time,
            status, wake_policy, sleep_policy, wake_lead_time_s,
            created_at_ms, updated_at_ms, origin, sync_state, paused,
            cloud_seq, vehicle_seq
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'vehicle', 'synced', $16, $17, $18)
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
            paused = EXCLUDED.paused,
            cloud_seq = EXCLUDED.cloud_seq,
            vehicle_seq = EXCLUDED.vehicle_seq,
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
            std::to_string(job.updated_at_ms()),
            job.paused() ? "true" : "false",
            std::to_string(job.version().cloud_seq()),
            std::to_string(job.version().vehicle_seq())
        });
}

void SchedulerStore::handle_v2_tombstone(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_v2::JobRecord& job) {

    LOG(INFO) << "Tombstone received: job_id=" << job.job_id()
              << " deleted_at_ms=" << job.deleted_at_ms();

    // Delete the job from the database
    // This handles both cloud-created and vehicle-created jobs
    auto result = db_->execute(
        "DELETE FROM jobs WHERE vehicle_id = $1 AND job_id = $2 RETURNING job_id",
        {vehicle_id, job.job_id()});

    if (result.ok() && result.num_rows() > 0) {
        LOG(INFO) << "Deleted job " << job.job_id() << " for vehicle " << vehicle_id;
    } else {
        VLOG(1) << "Tombstone for unknown job " << job.job_id()
                << " (already deleted or never existed)";
    }
}

void SchedulerStore::handle_v2_execution_record(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_v2::ExecutionRecord& exec) {

    LOG(INFO) << "Execution: " << exec.execution_id()
              << " job=" << exec.job_id()
              << " status=" << job_status_to_string(exec.status())
              << " duration=" << exec.duration_ms() << "ms";

    // Use execution_id for deduplication via unique index
    // ON CONFLICT on the partial unique index prevents duplicate execution records
    db_->execute(
        R"(
        INSERT INTO job_executions (
            vehicle_id, job_id, execution_id, status, executed_at_ms, duration_ms,
            result, error_message
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (execution_id) WHERE execution_id IS NOT NULL DO NOTHING
        )",
        {
            vehicle_id,
            exec.job_id(),
            exec.execution_id(),
            job_status_to_string(exec.status()),
            std::to_string(exec.executed_at_ms()),
            std::to_string(exec.duration_ms()),
            exec.result_json(),
            exec.error_message()
        });
}

std::vector<swdv::scheduler_sync_v2::JobRecord> SchedulerStore::get_pending_cloud_jobs(
    const std::string& vehicle_id) {

    std::vector<swdv::scheduler_sync_v2::JobRecord> jobs;

    // Get jobs with origin='cloud' and sync_state='pending'
    // These are jobs created by cloud that vehicle hasn't confirmed yet
    auto result = db_->execute(
        R"(
        SELECT job_id, title, service_name, method_name,
               COALESCE(parameters::text, '{}') as parameters,
               scheduled_time, recurrence_rule, next_run_time,
               status, wake_policy, sleep_policy, wake_lead_time_s,
               created_at_ms, updated_at_ms, end_time, paused,
               cloud_seq, vehicle_seq
        FROM jobs
        WHERE vehicle_id = $1 AND origin = 'cloud' AND sync_state = 'pending'
        )",
        {vehicle_id});

    if (!result.ok()) {
        LOG(ERROR) << "Failed to query pending cloud jobs: " << result.error();
        return jobs;
    }

    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);

        swdv::scheduler_sync_v2::JobRecord job;
        job.set_job_id(row.get_string(0));
        job.set_title(row.get_string(1));
        job.set_service(row.get_string(2));
        job.set_method(row.get_string(3));
        job.set_parameters_json(row.get_string(4));

        // Parse scheduled_time string to epoch milliseconds (ISO 8601)
        if (!row.is_null(5)) {
            uint64_t scheduled_ms = parse_timestamp_to_ms(row.get_string(5));
            if (scheduled_ms > 0) {
                job.set_scheduled_time_ms(scheduled_ms);
            }
        }

        job.set_recurrence_rule(row.get_string(6));

        // Parse next_run_time (ISO 8601)
        if (!row.is_null(7)) {
            uint64_t next_run_ms = parse_timestamp_to_ms(row.get_string(7));
            if (next_run_ms > 0) {
                job.set_next_run_time_ms(next_run_ms);
            }
        }

        job.set_status(string_to_job_status(row.get_string(8)));
        job.set_wake_policy(static_cast<swdv::scheduler_sync_v2::WakePolicy>(
            row.is_null(9) ? 0 : row.get_int(9)));
        job.set_sleep_policy(static_cast<swdv::scheduler_sync_v2::SleepPolicy>(
            row.is_null(10) ? 0 : row.get_int(10)));
        job.set_wake_lead_time_s(row.is_null(11) ? 0 : static_cast<uint32_t>(row.get_int(11)));
        job.set_created_at_ms(row.is_null(12) ? 0 : std::stoull(row.get_string(12)));
        job.set_updated_at_ms(row.is_null(13) ? 0 : std::stoull(row.get_string(13)));

        // Parse end_time (ISO 8601)
        if (!row.is_null(14)) {
            uint64_t end_time_ms = parse_timestamp_to_ms(row.get_string(14));
            if (end_time_ms > 0) {
                job.set_end_time_ms(end_time_ms);
            }
        }

        // Set paused state
        bool paused = !row.is_null(15) && (row.get_string(15) == "t" || row.get_string(15) == "true");
        job.set_paused(paused);

        // Set cloud authority for cloud-created jobs
        job.set_authority(swdv::scheduler_sync_v2::AUTHORITY_CLOUD);

        // Set version vector from database
        auto* version = job.mutable_version();
        version->set_cloud_seq(row.is_null(16) ? 0 : static_cast<uint64_t>(row.get_int64(16)));
        version->set_vehicle_seq(row.is_null(17) ? 0 : static_cast<uint64_t>(row.get_int64(17)));

        jobs.push_back(job);

        LOG(INFO) << "Pending cloud job: " << job.job_id() << " (" << job.title() << ")"
                  << " version={" << version->cloud_seq() << "," << version->vehicle_seq() << "}";
    }

    return jobs;
}

void SchedulerStore::mark_jobs_synced(const std::string& vehicle_id,
                                       const std::vector<std::string>& job_ids) {
    if (job_ids.empty()) return;

    // Build comma-separated list of quoted job_ids
    std::string job_ids_list;
    for (const auto& jid : job_ids) {
        if (!job_ids_list.empty()) job_ids_list += ",";
        job_ids_list += "'" + jid + "'";
    }

    db_->execute(
        "UPDATE jobs SET sync_state = 'synced', sync_updated_at = NOW() "
        "WHERE vehicle_id = $1 AND job_id IN (" + job_ids_list + ")",
        {vehicle_id});

    LOG(INFO) << "Marked " << job_ids.size() << " jobs as synced for " << vehicle_id;
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
        default:
            return "pending";  // Fallback for future enum values
    }
}

swdv::scheduler_sync_v2::JobStatus SchedulerStore::string_to_job_status(
    const std::string& status) {
    if (status == "pending") return swdv::scheduler_sync_v2::JOB_STATUS_PENDING;
    if (status == "running") return swdv::scheduler_sync_v2::JOB_STATUS_RUNNING;
    if (status == "completed") return swdv::scheduler_sync_v2::JOB_STATUS_COMPLETED;
    if (status == "failed") return swdv::scheduler_sync_v2::JOB_STATUS_FAILED;
    if (status == "cancelled") return swdv::scheduler_sync_v2::JOB_STATUS_CANCELLED;
    return swdv::scheduler_sync_v2::JOB_STATUS_PENDING;  // Default
}

// =============================================================================
// Quiescence Detection (Scheduler Sync Protocol v2.4 Section 5.6)
// =============================================================================

uint64_t SchedulerStore::compute_cloud_state_checksum(const std::string& vehicle_id) {
    // Query all jobs for this vehicle (both cloud and vehicle origin)
    // Per spec: checksum covers ALL jobs, not just pending
    auto result = db_->execute(
        R"(
        SELECT job_id, title, service_name, method_name,
               COALESCE(parameters::text, '{}') as parameters,
               scheduled_time, recurrence_rule, end_time,
               wake_policy, sleep_policy, wake_lead_time_s, paused
        FROM jobs
        WHERE vehicle_id = $1
        ORDER BY job_id  -- Deterministic ordering
        )",
        {vehicle_id});

    if (!result.ok()) {
        LOG(ERROR) << "Failed to query jobs for checksum: " << result.error();
        return 0;
    }

    // Convert DB rows to library Job structs
    std::vector<sched_lib::Job> jobs;
    jobs.reserve(result.num_rows());

    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);

        uint64_t scheduled_ms = 0;
        if (!row.is_null(5)) {
            scheduled_ms = parse_timestamp_to_ms(row.get_string(5));
        }

        uint64_t end_time_ms = 0;
        if (!row.is_null(7)) {
            end_time_ms = parse_timestamp_to_ms(row.get_string(7));
        }

        bool paused = !row.is_null(11) && (row.get_string(11) == "t" || row.get_string(11) == "true");

        jobs.push_back(make_job_for_hash(
            row.get_string(0),  // job_id
            row.get_string(1),  // title
            row.get_string(2),  // service_name
            row.get_string(3),  // method_name
            row.get_string(4),  // parameters
            scheduled_ms,
            row.get_string(6),  // recurrence_rule
            end_time_ms,
            paused,
            row.is_null(8) ? 0 : row.get_int(8),   // wake_policy
            row.is_null(9) ? 0 : row.get_int(9),   // sleep_policy
            row.is_null(10) ? 0 : static_cast<uint32_t>(row.get_int(10))  // wake_lead_time_s
        ));
    }

    // Jobs already sorted by job_id from SQL ORDER BY
    return sched_lib::compute_state_checksum(jobs);
}

void SchedulerStore::store_v2c_checksum(const std::string& vehicle_id, uint64_t v2c_checksum) {
    // Cast to signed int64_t for PostgreSQL BIGINT (preserves bit pattern)
    // Checksums with high bit set will appear negative but comparison still works
    int64_t signed_checksum = static_cast<int64_t>(v2c_checksum);

    // Ensure sync_state row exists
    db_->execute(
        "INSERT INTO sync_state (vehicle_id, scheduler_v2c_checksum, updated_at) "
        "VALUES ($1, $2, NOW()) "
        "ON CONFLICT (vehicle_id) DO UPDATE SET "
        "scheduler_v2c_checksum = $2, updated_at = NOW()",
        {vehicle_id, std::to_string(signed_checksum)});

    VLOG(1) << "Stored V2C checksum " << v2c_checksum << " (signed: " << signed_checksum << ") for " << vehicle_id;
}

uint64_t SchedulerStore::get_last_v2c_checksum(const std::string& vehicle_id) {
    auto result = db_->execute_scalar(
        "SELECT scheduler_v2c_checksum FROM sync_state WHERE vehicle_id = $1",
        {vehicle_id});

    if (result && !result->empty()) {
        // Convert from signed (PostgreSQL BIGINT) back to unsigned
        int64_t signed_val = std::stoll(*result);
        return static_cast<uint64_t>(signed_val);
    }
    return 0;  // No checksum stored yet
}

bool SchedulerStore::is_quiescent(const std::string& vehicle_id) {
    // Quiescent when no pending cloud jobs need to be synced to vehicle
    // Note: We don't compare cloud vs vehicle checksums - they may differ due to
    // different views (cloud has all jobs, vehicle may have terminal-only mode)
    // The vehicle's reported checksum is stored for tracking state changes over time
    auto result = db_->execute_scalar(
        "SELECT COUNT(*) FROM jobs WHERE vehicle_id = $1 AND origin = 'cloud' AND sync_state = 'pending'",
        {vehicle_id});

    if (result && std::stoi(*result) > 0) {
        return false;  // Have pending jobs to sync
    }

    return true;  // Quiescent - no pending cloud jobs
}

SchedulerStore::QuiescenceState SchedulerStore::get_quiescence_state(const std::string& vehicle_id) {
    QuiescenceState state;
    state.cloud_checksum = compute_cloud_state_checksum(vehicle_id);
    state.last_v2c_checksum = get_last_v2c_checksum(vehicle_id);

    // Quiescent when no pending cloud jobs to sync
    // Note: checksums are for tracking, not for comparison between cloud and vehicle
    auto result = db_->execute_scalar(
        "SELECT COUNT(*) FROM jobs WHERE vehicle_id = $1 AND origin = 'cloud' AND sync_state = 'pending'",
        {vehicle_id});
    state.is_quiescent = !(result && std::stoi(*result) > 0);

    return state;
}

}  // namespace ifex::offboard
