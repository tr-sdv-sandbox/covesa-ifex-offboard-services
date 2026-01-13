#include "scheduler_query.hpp"

#include <glog/logging.h>

namespace ifex {
namespace cloud {
namespace scheduler {

namespace {

/// Convert status string to integer for CloudJobStatus enum
/// Values: 0=unknown, 1=pending/active, 2=paused, 3=completed
int status_string_to_int(const std::string& status) {
    if (status == "pending" || status == "active") return 1;
    if (status == "paused") return 2;
    if (status == "completed") return 3;
    if (status == "failed") return 4;
    return 0;  // unknown
}

/// Convert sync_state string from jobs table to SyncState enum
query::SyncState sync_state_to_enum(const std::string& sync_state) {
    if (sync_state == "synced") {
        return query::SyncState::CONFIRMED;
    }
    if (sync_state == "pending") {
        return query::SyncState::PENDING;
    }
    return query::SyncState::UNKNOWN;
}

}  // namespace

SchedulerQuery::SchedulerQuery(std::shared_ptr<ifex::offboard::PostgresClient> db)
    : db_(std::move(db)) {}

SchedulerQueryResult<query::JobInfoData> SchedulerQuery::list_jobs(
    const std::string& vehicle_id_filter,
    const std::string& fleet_id_filter,
    const std::string& service_filter,
    int status_filter,
    int page_size,
    int offset) {

    // Query jobs table with sync_state tracking
    std::string sql = R"(
        SELECT j.job_id, j.vehicle_id, COALESCE(e.fleet_id, '') as fleet_id,
               COALESCE(e.region, '') as region, COALESCE(j.title, '') as title, j.service_name,
               j.method_name, COALESCE(j.parameters::text, '{}') as parameters,
               COALESCE(j.scheduled_time, '') as scheduled_time,
               COALESCE(j.recurrence_rule, '') as recurrence_rule, COALESCE(j.end_time, '') as end_time,
               j.status, j.created_at_ms, j.updated_at_ms, j.next_run_time, 0 as execution_count,
               COALESCE(j.sync_state, 'synced') as sync_state,
               CAST(EXTRACT(EPOCH FROM j.sync_updated_at) * 1000 AS BIGINT) as synced_at_ms,
               COALESCE(j.origin, 'vehicle') as origin
        FROM jobs j
        LEFT JOIN vehicle_enrichment e ON j.vehicle_id = e.vehicle_id
        WHERE 1=1
    )";

    std::string count_sql = R"(
        SELECT COUNT(*)
        FROM jobs j
        LEFT JOIN vehicle_enrichment e ON j.vehicle_id = e.vehicle_id
        WHERE 1=1
    )";

    std::vector<std::string> params;
    int param_idx = 1;

    if (!vehicle_id_filter.empty()) {
        std::string filter = " AND j.vehicle_id = $" + std::to_string(param_idx++);
        sql += filter;
        count_sql += filter;
        params.push_back(vehicle_id_filter);
    }
    if (!fleet_id_filter.empty()) {
        std::string filter = " AND e.fleet_id = $" + std::to_string(param_idx++);
        sql += filter;
        count_sql += filter;
        params.push_back(fleet_id_filter);
    }
    if (!service_filter.empty()) {
        std::string filter = " AND j.service_name = $" + std::to_string(param_idx++);
        sql += filter;
        count_sql += filter;
        params.push_back(service_filter);
    }
    if (status_filter >= 0) {
        std::string filter = " AND j.status = $" + std::to_string(param_idx++);
        sql += filter;
        count_sql += filter;
        params.push_back(std::to_string(status_filter));
    }

    // Get total count
    auto count_result = db_->execute(count_sql, params);
    int total_count = 0;
    if (count_result.num_rows() > 0) {
        total_count = count_result.row(0).get_int(0);
    }

    // Get paginated results
    sql += " ORDER BY j.created_at_ms DESC";
    sql += " LIMIT $" + std::to_string(param_idx++);
    params.push_back(std::to_string(page_size));
    sql += " OFFSET $" + std::to_string(param_idx++);
    params.push_back(std::to_string(offset));

    auto result = db_->execute(sql, params);
    SchedulerQueryResult<query::JobInfoData> query_result;
    query_result.total_count = total_count;

    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);
        query::JobInfoData job;
        job.job_id = row.get_string(0);
        job.vehicle_id = row.get_string(1);
        job.fleet_id = row.get_string(2);
        job.region = row.get_string(3);
        job.title = row.get_string(4);
        job.service_name = row.get_string(5);
        job.method_name = row.get_string(6);
        job.parameters_json = row.get_string(7);
        job.scheduled_time = row.get_string(8);
        job.recurrence_rule = row.get_string(9);
        job.end_time = row.get_string(10);
        job.status = status_string_to_int(row.get_string(11));
        job.created_at_ms = row.is_null(12) ? 0 : row.get_int64(12);
        job.updated_at_ms = row.is_null(13) ? 0 : row.get_int64(13);
        job.execution_count = row.is_null(15) ? 0 : row.get_int(15);
        // Sync state from jobs table
        std::string sync_state = row.is_null(16) ? "synced" : row.get_string(16);
        job.synced_at_ms = row.is_null(17) ? 0 : row.get_int64(17);
        job.sync_state = sync_state_to_enum(sync_state);
        query_result.items.push_back(std::move(job));
    }

    if (offset + static_cast<int>(query_result.items.size()) < total_count) {
        query_result.next_page_token = std::to_string(offset + page_size);
    }

    return query_result;
}

std::optional<query::JobInfoData> SchedulerQuery::get_job(const std::string& job_id) {
    const char* sql = R"(
        SELECT j.job_id, j.vehicle_id, COALESCE(e.fleet_id, '') as fleet_id,
               COALESCE(e.region, '') as region, COALESCE(j.title, '') as title, j.service_name,
               j.method_name, COALESCE(j.parameters::text, '{}') as parameters,
               COALESCE(j.scheduled_time, '') as scheduled_time,
               COALESCE(j.recurrence_rule, '') as recurrence_rule, COALESCE(j.end_time, '') as end_time,
               j.status, j.created_at_ms, j.updated_at_ms, j.next_run_time, 0 as execution_count,
               COALESCE(j.sync_state, 'synced') as sync_state,
               CAST(EXTRACT(EPOCH FROM j.sync_updated_at) * 1000 AS BIGINT) as synced_at_ms,
               COALESCE(j.origin, 'vehicle') as origin
        FROM jobs j
        LEFT JOIN vehicle_enrichment e ON j.vehicle_id = e.vehicle_id
        WHERE j.job_id = $1
    )";

    auto result = db_->execute(sql, {job_id});
    if (result.num_rows() == 0) {
        return std::nullopt;
    }

    auto row = result.row(0);
    query::JobInfoData job;
    job.job_id = row.get_string(0);
    job.vehicle_id = row.get_string(1);
    job.fleet_id = row.get_string(2);
    job.region = row.get_string(3);
    job.title = row.get_string(4);
    job.service_name = row.get_string(5);
    job.method_name = row.get_string(6);
    job.parameters_json = row.get_string(7);
    job.scheduled_time = row.get_string(8);
    job.recurrence_rule = row.get_string(9);
    job.end_time = row.get_string(10);
    job.status = status_string_to_int(row.get_string(11));
    job.created_at_ms = row.is_null(12) ? 0 : row.get_int64(12);
    job.updated_at_ms = row.is_null(13) ? 0 : row.get_int64(13);
    job.execution_count = row.is_null(15) ? 0 : row.get_int(15);
    // Sync state from jobs table
    std::string sync_state = row.is_null(16) ? "synced" : row.get_string(16);
    job.synced_at_ms = row.is_null(17) ? 0 : row.get_int64(17);
    job.sync_state = sync_state_to_enum(sync_state);

    return job;
}

std::vector<query::JobInfoData> SchedulerQuery::get_vehicle_jobs(const std::string& vehicle_id) {
    const char* sql = R"(
        SELECT j.job_id, j.vehicle_id, COALESCE(e.fleet_id, '') as fleet_id,
               COALESCE(e.region, '') as region, COALESCE(j.title, '') as title, j.service_name,
               j.method_name, COALESCE(j.parameters::text, '{}') as parameters,
               COALESCE(j.scheduled_time, '') as scheduled_time,
               COALESCE(j.recurrence_rule, '') as recurrence_rule, COALESCE(j.end_time, '') as end_time,
               j.status, j.created_at_ms, j.updated_at_ms, j.next_run_time, 0 as execution_count,
               COALESCE(j.sync_state, 'synced') as sync_state,
               CAST(EXTRACT(EPOCH FROM j.sync_updated_at) * 1000 AS BIGINT) as synced_at_ms
        FROM jobs j
        LEFT JOIN vehicle_enrichment e ON j.vehicle_id = e.vehicle_id
        WHERE j.vehicle_id = $1
        ORDER BY j.created_at_ms DESC
    )";

    auto result = db_->execute(sql, {vehicle_id});
    std::vector<query::JobInfoData> jobs;

    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);
        query::JobInfoData job;
        job.job_id = row.get_string(0);
        job.vehicle_id = row.get_string(1);
        job.fleet_id = row.get_string(2);
        job.region = row.get_string(3);
        job.title = row.get_string(4);
        job.service_name = row.get_string(5);
        job.method_name = row.get_string(6);
        job.parameters_json = row.get_string(7);
        job.scheduled_time = row.get_string(8);
        job.recurrence_rule = row.get_string(9);
        job.end_time = row.get_string(10);
        job.status = status_string_to_int(row.get_string(11));
        job.created_at_ms = row.is_null(12) ? 0 : row.get_int64(12);
        job.updated_at_ms = row.is_null(13) ? 0 : row.get_int64(13);
        job.execution_count = row.is_null(15) ? 0 : row.get_int(15);
        std::string sync_state = row.is_null(16) ? "synced" : row.get_string(16);
        job.synced_at_ms = row.is_null(17) ? 0 : row.get_int64(17);
        job.sync_state = sync_state_to_enum(sync_state);
        jobs.push_back(std::move(job));
    }

    return jobs;
}

SchedulerQueryResult<query::JobExecutionInfoData> SchedulerQuery::get_job_executions(
    const std::string& job_id,
    int page_size,
    int offset) {

    const char* count_sql = R"(
        SELECT COUNT(*) FROM job_executions WHERE job_id = $1
    )";

    auto count_result = db_->execute(count_sql, {job_id});
    int total_count = 0;
    if (count_result.num_rows() > 0) {
        total_count = count_result.row(0).get_int(0);
    }

    const char* sql = R"(
        SELECT execution_id, job_id, vehicle_id, status, started_at_ns,
               completed_at_ns, result_json, error_message
        FROM job_executions
        WHERE job_id = $1
        ORDER BY started_at_ns DESC
        LIMIT $2 OFFSET $3
    )";

    auto result = db_->execute(sql, {
        job_id,
        std::to_string(page_size),
        std::to_string(offset)
    });

    SchedulerQueryResult<query::JobExecutionInfoData> query_result;
    query_result.total_count = total_count;

    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);
        query::JobExecutionInfoData exec;
        exec.execution_id = row.get_string(0);
        exec.job_id = row.get_string(1);
        exec.vehicle_id = row.get_string(2);
        exec.status = row.get_int(3);
        exec.started_at_ns = row.get_int64(4);
        exec.completed_at_ns = row.get_int64(5);
        exec.result_json = row.get_string(6);
        exec.error_message = row.get_string(7);
        query_result.items.push_back(std::move(exec));
    }

    if (offset + static_cast<int>(query_result.items.size()) < total_count) {
        query_result.next_page_token = std::to_string(offset + page_size);
    }

    return query_result;
}

std::vector<query::FleetJobStatsData> SchedulerQuery::get_fleet_job_stats(
    const std::string& fleet_id_filter,
    const std::string& region_filter) {

    std::string sql = R"(
        SELECT j.service_name,
               COUNT(*) as total_jobs,
               COUNT(*) FILTER (WHERE j.status IN ('pending', 'active')) as active_jobs,
               COUNT(*) FILTER (WHERE j.status = 'paused') as paused_jobs,
               COUNT(*) FILTER (WHERE j.status = 'completed') as completed_jobs,
               COUNT(DISTINCT j.vehicle_id) as vehicle_count
        FROM jobs j
        LEFT JOIN vehicle_enrichment e ON j.vehicle_id = e.vehicle_id
        WHERE 1=1
    )";

    std::vector<std::string> params;
    int param_idx = 1;

    if (!fleet_id_filter.empty()) {
        sql += " AND e.fleet_id = $" + std::to_string(param_idx++);
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        sql += " AND e.region = $" + std::to_string(param_idx++);
        params.push_back(region_filter);
    }

    sql += " GROUP BY j.service_name ORDER BY total_jobs DESC";

    auto result = db_->execute(sql, params);
    std::vector<query::FleetJobStatsData> stats;

    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);
        query::FleetJobStatsData s;
        s.service_name = row.get_string(0);
        s.total_jobs = row.get_int(1);
        s.active_jobs = row.get_int(2);
        s.paused_jobs = row.get_int(3);
        s.completed_jobs = row.get_int(4);
        s.vehicle_count = row.get_int(5);
        stats.push_back(std::move(s));
    }

    return stats;
}

SchedulerQuery::JobCounts SchedulerQuery::get_job_counts(
    const std::string& fleet_id_filter,
    const std::string& region_filter) {

    std::string sql = R"(
        SELECT COUNT(*) as total,
               COUNT(*) FILTER (WHERE j.status IN ('pending', 'active')) as active,
               COUNT(*) FILTER (WHERE j.status = 'paused') as paused,
               COUNT(*) FILTER (WHERE j.status = 'completed') as completed
        FROM jobs j
        LEFT JOIN vehicle_enrichment e ON j.vehicle_id = e.vehicle_id
        WHERE 1=1
    )";

    std::vector<std::string> params;
    int param_idx = 1;

    if (!fleet_id_filter.empty()) {
        sql += " AND e.fleet_id = $" + std::to_string(param_idx++);
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        sql += " AND e.region = $" + std::to_string(param_idx++);
        params.push_back(region_filter);
    }

    auto result = db_->execute(sql, params);
    JobCounts counts;

    if (result.num_rows() > 0) {
        auto row = result.row(0);
        counts.total = row.get_int(0);
        counts.active = row.get_int(1);
        counts.paused = row.get_int(2);
        counts.completed = row.get_int(3);
    }

    return counts;
}

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
