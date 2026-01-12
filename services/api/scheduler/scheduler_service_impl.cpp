#include "scheduler_service_impl.hpp"

#include <glog/logging.h>
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>

namespace ifex {
namespace cloud {
namespace scheduler {

CloudSchedulerServiceImpl::CloudSchedulerServiceImpl(
    std::shared_ptr<ifex::offboard::PostgresClient> db,
    std::shared_ptr<JobCommandProducer> producer)
    : db_(std::move(db)),
      producer_(std::move(producer)),
      query_(db_) {}

std::string CloudSchedulerServiceImpl::generate_command_id() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dist;

    std::stringstream ss;
    ss << "cmd-" << std::hex << std::setfill('0') << std::setw(16) << dist(gen);
    return ss.str();
}

std::string CloudSchedulerServiceImpl::generate_job_id() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dist;

    std::stringstream ss;
    ss << "job-" << std::hex << std::setfill('0') << std::setw(16) << dist(gen);
    return ss.str();
}

// Parse ISO8601 datetime string to epoch milliseconds
// Returns 0 if the string is empty or cannot be parsed
static uint64_t Iso8601ToEpochMs(const std::string& iso_str) {
    if (iso_str.empty()) {
        return 0;
    }

    std::tm tm = {};
    std::istringstream ss(iso_str);

    // Try parsing with milliseconds: 2026-01-09T17:00:00.000Z
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (ss.fail()) {
        LOG(WARNING) << "Failed to parse ISO8601 datetime: " << iso_str;
        return 0;
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
        // Handle variable precision (e.g., .1, .12, .123)
        std::string remaining = std::to_string(frac);
        while (remaining.length() < 3) remaining += "0";
        ms = std::stoull(remaining.substr(0, 3));
    }

    return static_cast<uint64_t>(epoch_sec) * 1000 + ms;
}

grpc::Status CloudSchedulerServiceImpl::CreateJob(
    grpc::ServerContext* context,
    const CreateJobRequest* request,
    CreateJobResponse* response) {

    if (request->vehicle_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "vehicle_id is required");
    }
    if (request->service().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service is required");
    }
    if (request->method().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method is required");
    }

    std::string job_id = generate_job_id();
    std::string command_id = generate_command_id();

    LOG(INFO) << "CreateJob: vehicle=" << request->vehicle_id()
              << " job=" << job_id
              << " method=" << request->service() << "." << request->method();

    try {
        // Convert ISO8601 strings to epoch milliseconds
        uint64_t scheduled_time_ms = Iso8601ToEpochMs(request->scheduled_time());
        uint64_t end_time_ms = Iso8601ToEpochMs(request->end_time());

        bool sent = producer_->send_create_job(
            request->vehicle_id(),
            command_id,
            job_id,
            request->title(),
            request->service(),
            request->method(),
            request->parameters_json(),
            scheduled_time_ms,
            request->recurrence_rule(),
            end_time_ms,
            request->created_by());

        if (sent) {
            // Store job in cloud database for immediate query access
            // Note: Vehicle must already exist - created via status tracking (is_online topic)
            auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            // Default empty parameters to valid JSON object
            std::string params_json = request->parameters_json();
            if (params_json.empty()) {
                params_json = "{}";
            }

            auto result = db_->execute(R"(
                INSERT INTO jobs (vehicle_id, job_id, title, service_name, method_name,
                                  parameters, scheduled_time, recurrence_rule,
                                  status, created_at_ms, updated_at_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, $9)
                ON CONFLICT (vehicle_id, job_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    service_name = EXCLUDED.service_name,
                    method_name = EXCLUDED.method_name,
                    parameters = EXCLUDED.parameters,
                    scheduled_time = EXCLUDED.scheduled_time,
                    recurrence_rule = EXCLUDED.recurrence_rule,
                    updated_at_ms = EXCLUDED.updated_at_ms
            )", {
                request->vehicle_id(),
                job_id,
                request->title(),
                request->service(),
                request->method(),
                params_json,
                request->scheduled_time(),
                request->recurrence_rule(),
                std::to_string(now_ms)
            });

            if (!result.ok()) {
                LOG(WARNING) << "Failed to store job in cloud DB: " << result.error()
                             << " (vehicle may not exist yet)";
            }
        }

        response->set_job_id(job_id);
        response->set_success(sent);

        if (!sent) {
            response->set_error_message("Failed to send command to Kafka");
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "CreateJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::UpdateJob(
    grpc::ServerContext* context,
    const UpdateJobRequest* request,
    UpdateJobResponse* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        // Look up job to get vehicle_id
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "UpdateJob: job=" << request->job_id()
                  << " vehicle=" << job->vehicle_id;

        // Convert ISO8601 strings to epoch milliseconds
        uint64_t scheduled_time_ms = Iso8601ToEpochMs(request->scheduled_time());
        uint64_t end_time_ms = Iso8601ToEpochMs(request->end_time());

        bool sent = producer_->send_update_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            request->title(),
            scheduled_time_ms,
            request->recurrence_rule(),
            request->parameters_json(),
            end_time_ms,
            "");  // No requester_id in proto

        response->set_success(sent);

        if (!sent) {
            response->set_error_message("Failed to send command to Kafka");
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "UpdateJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::DeleteJob(
    grpc::ServerContext* context,
    const DeleteJobRequest* request,
    DeleteJobResponse* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "DeleteJob: job=" << request->job_id()
                  << " vehicle=" << job->vehicle_id;

        bool sent = producer_->send_delete_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            "");  // No requester_id in proto

        response->set_success(sent);

        if (!sent) {
            response->set_error_message("Failed to send command to Kafka");
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "DeleteJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::PauseJob(
    grpc::ServerContext* context,
    const PauseJobRequest* request,
    PauseJobResponse* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "PauseJob: job=" << request->job_id();

        bool sent = producer_->send_pause_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            "");  // No requester_id in proto

        response->set_success(sent);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "PauseJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::ResumeJob(
    grpc::ServerContext* context,
    const ResumeJobRequest* request,
    ResumeJobResponse* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "ResumeJob: job=" << request->job_id();

        bool sent = producer_->send_resume_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            "");  // No requester_id in proto

        response->set_success(sent);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "ResumeJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::TriggerJob(
    grpc::ServerContext* context,
    const TriggerJobRequest* request,
    TriggerJobResponse* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "TriggerJob: job=" << request->job_id();

        bool sent = producer_->send_trigger_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            "");  // No requester_id in proto

        response->set_success(sent);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "TriggerJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::GetJob(
    grpc::ServerContext* context,
    const GetJobRequest* request,
    GetJobResponse* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        response->set_found(true);
        auto* j = response->mutable_job();
        j->set_job_id(job->job_id);
        j->set_vehicle_id(job->vehicle_id);
        j->set_fleet_id(job->fleet_id);
        j->set_region(job->region);
        j->set_title(job->title);
        j->set_service(job->service_name);
        j->set_method(job->method_name);
        j->set_parameters_json(job->parameters_json);
        j->set_scheduled_time(job->scheduled_time);
        j->set_recurrence_rule(job->recurrence_rule);
        j->set_status(static_cast<::ifex::cloud::scheduler::CloudJobStatus>(job->status));
        j->set_created_at_ms(job->created_at_ns / 1000000);
        j->set_updated_at_ms(job->updated_at_ns / 1000000);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "GetJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::ListJobs(
    grpc::ServerContext* context,
    const ListJobsRequest* request,
    ListJobsResponse* response) {

    int page_size = request->page_size();
    if (page_size <= 0 || page_size > 1000) {
        page_size = 100;
    }

    int offset = 0;
    if (!request->page_token().empty()) {
        try {
            offset = std::stoi(request->page_token());
        } catch (...) {
            return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid page token");
        }
    }

    try {
        auto result = query_.list_jobs(
            request->vehicle_id_filter(),
            request->fleet_id_filter(),
            request->service_filter(),
            request->status_filter() != ::ifex::cloud::scheduler::CLOUD_JOB_UNKNOWN ?
                static_cast<int>(request->status_filter()) : -1,
            page_size,
            offset);

        for (const auto& job : result.items) {
            auto* j = response->add_jobs();
            j->set_job_id(job.job_id);
            j->set_vehicle_id(job.vehicle_id);
            j->set_fleet_id(job.fleet_id);
            j->set_region(job.region);
            j->set_title(job.title);
            j->set_service(job.service_name);
            j->set_method(job.method_name);
            j->set_status(static_cast<::ifex::cloud::scheduler::CloudJobStatus>(job.status));
            j->set_created_at_ms(job.created_at_ns / 1000000);
        }

        response->set_total_count(result.total_count);
        response->set_next_page_token(result.next_page_token);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "ListJobs failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::GetJobExecutions(
    grpc::ServerContext* context,
    const GetJobExecutionsRequest* request,
    GetJobExecutionsResponse* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    int limit = request->limit();
    if (limit <= 0 || limit > 1000) {
        limit = 100;
    }

    try {
        // Query with limit and since_ms filter
        auto result = query_.get_job_executions(request->job_id(), limit, 0);

        response->set_vehicle_id(request->vehicle_id());
        response->set_job_id(request->job_id());

        for (const auto& exec : result.items) {
            // Skip executions before since_ms if specified
            if (request->since_ms() > 0 &&
                static_cast<uint64_t>(exec.started_at_ns / 1000000) < request->since_ms()) {
                continue;
            }
            auto* e = response->add_executions();
            e->set_execution_id(exec.execution_id);
            e->set_status(static_cast<::ifex::cloud::scheduler::CloudJobStatus>(exec.status));
            e->set_executed_at_ms(exec.started_at_ns / 1000000);
            e->set_duration_ms(static_cast<uint32_t>((exec.completed_at_ns - exec.started_at_ns) / 1000000));
            e->set_result_json(exec.result_json);
            e->set_error_message(exec.error_message);
        }

        response->set_total_count(result.total_count);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "GetJobExecutions failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::CreateFleetJob(
    grpc::ServerContext* context,
    const CreateFleetJobRequest* request,
    CreateFleetJobResponse* response) {

    if (request->vehicle_ids().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "At least one vehicle_id is required");
    }
    if (request->service().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service is required");
    }
    if (request->method().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method is required");
    }

    LOG(INFO) << "CreateFleetJob: " << request->vehicle_ids().size() << " vehicles"
              << " method=" << request->service() << "." << request->method();

    // Convert ISO8601 strings to epoch milliseconds (once, outside loop)
    uint64_t scheduled_time_ms = Iso8601ToEpochMs(request->scheduled_time());
    uint64_t end_time_ms = Iso8601ToEpochMs(request->end_time());

    int success_count = 0;
    int fail_count = 0;

    try {
        for (const auto& vehicle_id : request->vehicle_ids()) {
            std::string job_id = generate_job_id();
            std::string command_id = generate_command_id();

            bool sent = producer_->send_create_job(
                vehicle_id,
                command_id,
                job_id,
                request->title(),
                request->service(),
                request->method(),
                request->parameters_json(),
                scheduled_time_ms,
                request->recurrence_rule(),
                end_time_ms,
                request->created_by());

            auto* result = response->add_results();
            result->set_vehicle_id(vehicle_id);
            result->set_job_id(job_id);
            result->set_success(sent);

            if (sent) {
                success_count++;
            } else {
                fail_count++;
                result->set_error_message("Failed to send to Kafka");
            }
        }

        response->set_total_vehicles(response->results_size());
        response->set_successful(success_count);
        response->set_failed(fail_count);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "CreateFleetJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::DeleteFleetJob(
    grpc::ServerContext* context,
    const DeleteFleetJobRequest* request,
    DeleteFleetJobResponse* response) {

    if (request->job_ids().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "At least one job_id is required");
    }

    LOG(INFO) << "DeleteFleetJob: " << request->job_ids().size() << " jobs";

    int success_count = 0;
    int fail_count = 0;

    try {
        for (const auto& job_id : request->job_ids()) {
            auto job = query_.get_job(job_id);
            if (!job) {
                fail_count++;
                continue;
            }

            std::string command_id = generate_command_id();

            bool sent = producer_->send_delete_job(
                job->vehicle_id,
                command_id,
                job_id,
                "");  // No requester_id in proto

            if (sent) {
                success_count++;
            } else {
                fail_count++;
            }
        }

        response->set_total_deletions(success_count + fail_count);
        response->set_successful(success_count);
        response->set_failed(fail_count);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "DeleteFleetJob failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::GetFleetJobStats(
    grpc::ServerContext* context,
    const GetFleetJobStatsRequest* request,
    GetFleetJobStatsResponse* response) {

    try {
        // Get per-service stats
        auto stats = query_.get_fleet_job_stats(
            request->fleet_id_filter(),
            request->region_filter());

        int total_vehicles = 0;
        for (const auto& s : stats) {
            auto* stat = response->add_by_service_method();
            stat->set_service(s.service_name);
            stat->set_total_jobs(s.total_jobs);
            stat->set_pending(s.active_jobs);
            stat->set_completed(s.completed_jobs);
            total_vehicles += s.vehicle_count;
        }

        // Get overall counts
        auto counts = query_.get_job_counts(
            request->fleet_id_filter(),
            request->region_filter());

        response->set_total_jobs(counts.total);
        response->set_total_vehicles_with_jobs(total_vehicles);
        (*response->mutable_by_status())["active"] = counts.active;
        (*response->mutable_by_status())["paused"] = counts.paused;
        (*response->mutable_by_status())["completed"] = counts.completed;

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "GetFleetJobStats failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
