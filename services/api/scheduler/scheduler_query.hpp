#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "postgres_client.hpp"

namespace ifex {
namespace cloud {
namespace scheduler {

// Data structs for query results (distinct from protobuf-generated classes)
namespace query {

/// Job information for listing
struct JobInfoData {
    std::string job_id;
    std::string vehicle_id;
    std::string fleet_id;
    std::string region;
    std::string title;
    std::string service_name;
    std::string method_name;
    std::string parameters_json;
    std::string scheduled_time;
    std::string recurrence_rule;
    std::string end_time;
    int status = 0;
    int64_t created_at_ns = 0;
    int64_t updated_at_ns = 0;
    int64_t next_run_ns = 0;
    int execution_count = 0;
};

/// Job execution info
struct JobExecutionInfoData {
    std::string execution_id;
    std::string job_id;
    std::string vehicle_id;
    int status = 0;
    int64_t started_at_ns = 0;
    int64_t completed_at_ns = 0;
    std::string result_json;
    std::string error_message;
};

/// Fleet job statistics
struct FleetJobStatsData {
    std::string service_name;
    int total_jobs = 0;
    int active_jobs = 0;
    int paused_jobs = 0;
    int completed_jobs = 0;
    int vehicle_count = 0;
};

}  // namespace query

/// Query result with pagination
template<typename T>
struct SchedulerQueryResult {
    std::vector<T> items;
    int total_count = 0;
    std::string next_page_token;
};

/**
 * PostgreSQL query helper for scheduler data
 */
class SchedulerQuery {
public:
    explicit SchedulerQuery(std::shared_ptr<ifex::offboard::PostgresClient> db);

    /// List jobs with filters
    SchedulerQueryResult<query::JobInfoData> list_jobs(
        const std::string& vehicle_id_filter,
        const std::string& fleet_id_filter,
        const std::string& service_filter,
        int status_filter,
        int page_size,
        int offset);

    /// Get a specific job
    std::optional<query::JobInfoData> get_job(const std::string& job_id);

    /// Get jobs for a vehicle
    std::vector<query::JobInfoData> get_vehicle_jobs(const std::string& vehicle_id);

    /// Get job executions
    SchedulerQueryResult<query::JobExecutionInfoData> get_job_executions(
        const std::string& job_id,
        int page_size,
        int offset);

    /// Get fleet-wide job statistics
    std::vector<query::FleetJobStatsData> get_fleet_job_stats(
        const std::string& fleet_id_filter,
        const std::string& region_filter);

    /// Count jobs by status
    struct JobCounts {
        int total = 0;
        int active = 0;
        int paused = 0;
        int completed = 0;
    };
    JobCounts get_job_counts(
        const std::string& fleet_id_filter,
        const std::string& region_filter);

private:
    std::shared_ptr<ifex::offboard::PostgresClient> db_;
};

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
