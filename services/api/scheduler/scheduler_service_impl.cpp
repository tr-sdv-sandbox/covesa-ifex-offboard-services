#include "scheduler_service_impl.hpp"

#include <glog/logging.h>
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>

namespace ifex::cloud::scheduler {

CloudSchedulerServiceImpl::CloudSchedulerServiceImpl(
    std::shared_ptr<ifex::offboard::PostgresClient> db,
    std::shared_ptr<JobCommandProducer> producer)
    : db_(std::move(db)),
      producer_(std::move(producer)),
      query_(db_),
      is_healthy_(true) {}

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

proto::cloud_job_status_t CloudSchedulerServiceImpl::map_status(int status) {
    switch (status) {
        case 0: return proto::JOB_UNKNOWN;
        case 1: return proto::JOB_PENDING;
        case 2: return proto::JOB_SCHEDULED;
        case 3: return proto::JOB_RUNNING;
        case 4: return proto::JOB_COMPLETED;
        case 5: return proto::JOB_FAILED;
        case 6: return proto::JOB_CANCELLED;
        case 7: return proto::JOB_PAUSED;
        default: return proto::JOB_UNKNOWN;
    }
}

proto::sync_state_t CloudSchedulerServiceImpl::map_sync_state(query::SyncState state) {
    switch (state) {
        case query::SyncState::UNKNOWN: return proto::SYNC_UNKNOWN;
        case query::SyncState::PENDING: return proto::SYNC_PENDING;
        case query::SyncState::CONFIRMED: return proto::SYNC_CONFIRMED;
        case query::SyncState::FAILED: return proto::SYNC_FAILED;
        case query::SyncState::OUT_OF_SYNC: return proto::SYNC_OUT_OF_SYNC;
        default: return proto::SYNC_UNKNOWN;
    }
}

// Convert epoch milliseconds to ISO8601 datetime string
// Returns empty string if ms is 0
static std::string EpochMsToIso8601(uint64_t ms) {
    if (ms == 0) {
        return "";
    }

    time_t epoch_sec = static_cast<time_t>(ms / 1000);
    uint64_t remainder_ms = ms % 1000;

    std::tm tm;
    gmtime_r(&epoch_sec, &tm);

    std::ostringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
    ss << '.' << std::setfill('0') << std::setw(3) << remainder_ms << 'Z';

    return ss.str();
}

// Parse ISO8601 datetime string to epoch milliseconds
// Returns 0 if the string is empty or cannot be parsed
// Handles formats:
//   - 2026-01-09T17:00:00.000Z (ISO8601 with T and optional ms)
//   - 2026-01-09 17:00:00+00 (PostgreSQL format with space)
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

    // Parse optional milliseconds or timezone suffix
    uint64_t ms = 0;
    char c;
    if (ss >> c) {
        if (c == '.') {
            // Parse milliseconds
            int frac;
            ss >> frac;
            // Handle variable precision (e.g., .1, .12, .123)
            std::string remaining = std::to_string(frac);
            while (remaining.length() < 3) remaining += "0";
            ms = std::stoull(remaining.substr(0, 3));
        }
        // Ignore timezone suffixes like 'Z' or '+00'
    }

    return static_cast<uint64_t>(epoch_sec) * 1000 + ms;
}

grpc::Status CloudSchedulerServiceImpl::create_job(
    grpc::ServerContext* context,
    const proto::create_job_request* request,
    proto::create_job_response* response) {

    const auto& req = request->request();

    if (req.vehicle_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "vehicle_id is required");
    }
    if (req.service().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service is required");
    }
    if (req.method().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method is required");
    }
    if (req.scheduled_time_ms() == 0) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "scheduled_time_ms is required");
    }

    std::string job_id = generate_job_id();
    std::string command_id = generate_command_id();

    LOG(INFO) << "create_job: vehicle=" << req.vehicle_id()
              << " job=" << job_id
              << " method=" << req.service() << "." << req.method()
              << " scheduled_time_ms=" << req.scheduled_time_ms();

    try {
        // Use milliseconds directly from proto
        uint64_t scheduled_time_ms = req.scheduled_time_ms();
        uint64_t end_time_ms = req.end_time_ms();

        bool sent = producer_->send_create_job(
            req.vehicle_id(),
            command_id,
            job_id,
            req.title(),
            req.service(),
            req.method(),
            req.parameters_json(),
            scheduled_time_ms,
            req.recurrence_rule(),
            end_time_ms,
            req.created_by());

        if (sent) {
            // Store job in jobs table with origin='cloud', sync_state='pending'
            // Note: Vehicle must already exist - created via status tracking (is_online topic)
            auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            // Default empty parameters to valid JSON object
            std::string params_json = req.parameters_json();
            if (params_json.empty()) {
                params_json = "{}";
            }

            auto result = db_->execute(R"(
                INSERT INTO jobs (
                    vehicle_id, job_id, title, service_name, method_name,
                    parameters, scheduled_time, recurrence_rule, end_time,
                    status, origin, sync_state, created_by, created_at_ms, updated_at_ms
                )
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, 'pending', 'cloud', 'pending', $10, $11, $11)
                ON CONFLICT (vehicle_id, job_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    service_name = EXCLUDED.service_name,
                    method_name = EXCLUDED.method_name,
                    parameters = EXCLUDED.parameters,
                    scheduled_time = EXCLUDED.scheduled_time,
                    recurrence_rule = EXCLUDED.recurrence_rule,
                    end_time = EXCLUDED.end_time,
                    sync_state = 'pending',
                    updated_at_ms = EXCLUDED.updated_at_ms,
                    sync_updated_at = NOW()
            )", {
                req.vehicle_id(),
                job_id,
                req.title(),
                req.service(),
                req.method(),
                params_json,
                EpochMsToIso8601(scheduled_time_ms),
                req.recurrence_rule(),
                EpochMsToIso8601(end_time_ms),
                req.created_by(),
                std::to_string(now_ms)
            });

            if (!result.ok()) {
                LOG(ERROR) << "Failed to store job in DB: " << result.error();
                auto* res = response->mutable_result();
                res->set_job_id(job_id);
                res->set_success(false);
                res->set_error_message("Failed to store job in database: " + result.error());
                return grpc::Status::OK;
            }
        }

        auto* res = response->mutable_result();
        res->set_job_id(job_id);
        res->set_success(sent);

        if (!sent) {
            res->set_error_message("Failed to send command to Kafka");
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "create_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::update_job(
    grpc::ServerContext* context,
    const proto::update_job_request* request,
    proto::update_job_response* response) {

    const auto& req = request->request();

    if (req.job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        // Look up job to get vehicle_id
        auto job = query_.get_job(req.job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "update_job: job=" << req.job_id()
                  << " vehicle=" << job->vehicle_id;

        // Use milliseconds directly from proto
        uint64_t scheduled_time_ms = req.scheduled_time_ms();
        uint64_t end_time_ms = req.end_time_ms();

        bool sent = producer_->send_update_job(
            job->vehicle_id,
            command_id,
            req.job_id(),
            req.title(),
            scheduled_time_ms,
            req.recurrence_rule(),
            req.parameters_json(),
            end_time_ms,
            "");  // No requester_id in proto

        if (sent) {
            // Update job in DB and set sync_state='pending'
            auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            auto result = db_->execute(R"(
                UPDATE jobs SET
                    title = COALESCE(NULLIF($2, ''), title),
                    scheduled_time = COALESCE(NULLIF($3, ''), scheduled_time),
                    recurrence_rule = COALESCE(NULLIF($4, ''), recurrence_rule),
                    parameters = COALESCE(NULLIF($5, '{}')::jsonb, parameters),
                    end_time = COALESCE(NULLIF($6, ''), end_time),
                    sync_state = 'pending',
                    updated_at_ms = $7,
                    sync_updated_at = NOW()
                WHERE job_id = $1
            )", {
                req.job_id(),
                req.title(),
                EpochMsToIso8601(scheduled_time_ms),
                req.recurrence_rule(),
                req.parameters_json().empty() ? "{}" : req.parameters_json(),
                EpochMsToIso8601(end_time_ms),
                std::to_string(now_ms)
            });

            if (!result.ok()) {
                LOG(ERROR) << "Failed to update job in DB: " << result.error();
                auto* res = response->mutable_result();
                res->set_success(false);
                res->set_error_message("Failed to update job in database: " + result.error());
                return grpc::Status::OK;
            }
        }

        auto* res = response->mutable_result();
        res->set_success(sent);

        if (!sent) {
            res->set_error_message("Failed to send command to Kafka");
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "update_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::delete_job(
    grpc::ServerContext* context,
    const proto::delete_job_request* request,
    proto::delete_job_response* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "delete_job: job=" << request->job_id()
                  << " vehicle=" << job->vehicle_id;

        bool sent = producer_->send_delete_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            "");  // No requester_id in proto

        if (sent) {
            // Mark job as pending deletion - will be removed when vehicle syncs
            db_->execute(R"(
                UPDATE jobs SET
                    status = 'deleting',
                    sync_state = 'pending',
                    sync_updated_at = NOW()
                WHERE job_id = $1
            )", {request->job_id()});
        }

        auto* res = response->mutable_result();
        res->set_success(sent);

        if (!sent) {
            res->set_error_message("Failed to send command to Kafka");
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "delete_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::pause_job(
    grpc::ServerContext* context,
    const proto::pause_job_request* request,
    proto::pause_job_response* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "pause_job: job=" << request->job_id();

        bool sent = producer_->send_pause_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            "");  // No requester_id in proto

        auto* res = response->mutable_result();
        res->set_success(sent);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "pause_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::resume_job(
    grpc::ServerContext* context,
    const proto::resume_job_request* request,
    proto::resume_job_response* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "resume_job: job=" << request->job_id();

        bool sent = producer_->send_resume_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            "");  // No requester_id in proto

        auto* res = response->mutable_result();
        res->set_success(sent);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "resume_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::trigger_job(
    grpc::ServerContext* context,
    const proto::trigger_job_request* request,
    proto::trigger_job_response* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        if (!job) {
            return grpc::Status(grpc::NOT_FOUND, "Job not found");
        }

        std::string command_id = generate_command_id();

        LOG(INFO) << "trigger_job: job=" << request->job_id();

        bool sent = producer_->send_trigger_job(
            job->vehicle_id,
            command_id,
            request->job_id(),
            "");  // No requester_id in proto

        auto* res = response->mutable_result();
        res->set_success(sent);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "trigger_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::get_job(
    grpc::ServerContext* context,
    const proto::get_job_request* request,
    proto::get_job_response* response) {

    if (request->job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    try {
        auto job = query_.get_job(request->job_id());
        auto* res = response->mutable_result();
        if (!job) {
            res->set_found(false);
            return grpc::Status::OK;
        }

        res->set_found(true);
        auto* j = res->mutable_job();
        j->set_job_id(job->job_id);
        j->set_vehicle_id(job->vehicle_id);
        j->set_fleet_id(job->fleet_id);
        j->set_region(job->region);
        j->set_title(job->title);
        j->set_service(job->service_name);
        j->set_method(job->method_name);
        j->set_parameters_json(job->parameters_json);
        j->set_scheduled_time_ms(Iso8601ToEpochMs(job->scheduled_time));
        j->set_recurrence_rule(job->recurrence_rule);
        j->set_status(map_status(job->status));
        j->set_created_at_ms(job->created_at_ms);
        j->set_updated_at_ms(job->updated_at_ms);
        j->set_sync_state(map_sync_state(job->sync_state));
        j->set_synced_at_ms(job->synced_at_ms);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "get_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::list_jobs(
    grpc::ServerContext* context,
    const proto::list_jobs_request* request,
    proto::list_jobs_response* response) {

    const auto& filter = request->filter();

    int page_size = filter.page_size();
    if (page_size <= 0 || page_size > 1000) {
        page_size = 100;
    }

    int offset = 0;
    if (!filter.page_token().empty()) {
        try {
            offset = std::stoi(filter.page_token());
        } catch (...) {
            return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid page token");
        }
    }

    try {
        auto result = query_.list_jobs(
            filter.vehicle_id_filter(),
            filter.fleet_id_filter(),
            filter.service_filter(),
            filter.status_filter() != proto::JOB_UNKNOWN ?
                static_cast<int>(filter.status_filter()) : -1,
            page_size,
            offset);

        auto* res = response->mutable_result();
        for (const auto& job : result.items) {
            auto* j = res->add_jobs();
            j->set_job_id(job.job_id);
            j->set_vehicle_id(job.vehicle_id);
            j->set_fleet_id(job.fleet_id);
            j->set_region(job.region);
            j->set_title(job.title);
            j->set_service(job.service_name);
            j->set_method(job.method_name);
            j->set_parameters_json(job.parameters_json);
            j->set_status(map_status(job.status));
            j->set_scheduled_time_ms(Iso8601ToEpochMs(job.scheduled_time));
            j->set_recurrence_rule(job.recurrence_rule);
            j->set_created_at_ms(job.created_at_ms);
            j->set_updated_at_ms(job.updated_at_ms);
            j->set_sync_state(map_sync_state(job.sync_state));
            j->set_synced_at_ms(job.synced_at_ms);
        }

        res->set_total_count(result.total_count);
        res->set_next_page_token(result.next_page_token);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "list_jobs failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::get_job_executions(
    grpc::ServerContext* context,
    const proto::get_job_executions_request* request,
    proto::get_job_executions_response* response) {

    const auto& req = request->request();

    if (req.job_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "job_id is required");
    }

    int limit = req.limit();
    if (limit <= 0 || limit > 1000) {
        limit = 100;
    }

    try {
        // Query with limit and since_ms filter
        auto result = query_.get_job_executions(req.job_id(), limit, 0);

        auto* res = response->mutable_result();
        res->set_vehicle_id(req.vehicle_id());
        res->set_job_id(req.job_id());

        for (const auto& exec : result.items) {
            // Skip executions before since_ms if specified
            if (req.since_ms() > 0 &&
                static_cast<uint64_t>(exec.started_at_ns / 1000000) < req.since_ms()) {
                continue;
            }
            auto* e = res->add_executions();
            e->set_execution_id(exec.execution_id);
            e->set_status(map_status(exec.status));
            e->set_executed_at_ms(exec.started_at_ns / 1000000);
            e->set_duration_ms(static_cast<uint32_t>((exec.completed_at_ns - exec.started_at_ns) / 1000000));
            e->set_result_json(exec.result_json);
            e->set_error_message(exec.error_message);
        }

        res->set_total_count(result.total_count);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "get_job_executions failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::create_fleet_job(
    grpc::ServerContext* context,
    const proto::create_fleet_job_request* request,
    proto::create_fleet_job_response* response) {

    const auto& req = request->request();

    if (req.vehicle_ids().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "At least one vehicle_id is required");
    }
    if (req.service().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service is required");
    }
    if (req.method().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method is required");
    }

    LOG(INFO) << "create_fleet_job: " << req.vehicle_ids().size() << " vehicles"
              << " method=" << req.service() << "." << req.method();

    // Use milliseconds directly from proto
    uint64_t scheduled_time_ms = req.scheduled_time_ms();
    uint64_t end_time_ms = req.end_time_ms();

    int success_count = 0;
    int fail_count = 0;

    try {
        auto* res = response->mutable_result();

        for (const auto& vehicle_id : req.vehicle_ids()) {
            std::string job_id = generate_job_id();
            std::string command_id = generate_command_id();

            bool sent = producer_->send_create_job(
                vehicle_id,
                command_id,
                job_id,
                req.title(),
                req.service(),
                req.method(),
                req.parameters_json(),
                scheduled_time_ms,
                req.recurrence_rule(),
                end_time_ms,
                req.created_by());

            auto* result = res->add_results();
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

        res->set_total_vehicles(res->results_size());
        res->set_successful(success_count);
        res->set_failed(fail_count);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "create_fleet_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::delete_fleet_job(
    grpc::ServerContext* context,
    const proto::delete_fleet_job_request* request,
    proto::delete_fleet_job_response* response) {

    const auto& req = request->request();

    if (req.job_ids().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "At least one job_id is required");
    }

    LOG(INFO) << "delete_fleet_job: " << req.job_ids().size() << " jobs";

    int success_count = 0;
    int fail_count = 0;

    try {
        for (const auto& job_id : req.job_ids()) {
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

        auto* res = response->mutable_result();
        res->set_total_deletions(success_count + fail_count);
        res->set_successful(success_count);
        res->set_failed(fail_count);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "delete_fleet_job failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::get_fleet_job_stats(
    grpc::ServerContext* context,
    const proto::get_fleet_job_stats_request* request,
    proto::get_fleet_job_stats_response* response) {

    const auto& filter = request->filter();

    try {
        // Get per-service stats
        auto stats = query_.get_fleet_job_stats(
            filter.fleet_id_filter(),
            filter.region_filter());

        auto* res = response->mutable_result();
        int total_vehicles = 0;
        for (const auto& s : stats) {
            auto* stat = res->add_by_service_method();
            stat->set_service(s.service_name);
            stat->set_total_jobs(s.total_jobs);
            stat->set_pending(s.active_jobs);
            stat->set_completed(s.completed_jobs);
            total_vehicles += s.vehicle_count;
        }

        // Get overall counts
        auto counts = query_.get_job_counts(
            filter.fleet_id_filter(),
            filter.region_filter());

        res->set_total_jobs(counts.total);
        res->set_total_vehicles_with_jobs(total_vehicles);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "get_fleet_job_stats failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudSchedulerServiceImpl::healthy(
    grpc::ServerContext* context,
    const proto::healthy_request* request,
    proto::healthy_response* response) {

    // Check if all dependencies are healthy
    bool healthy = is_healthy_ && db_ && db_->is_connected() && producer_;

    response->set_is_healthy(healthy);

    return grpc::Status::OK;
}

}  // namespace ifex::cloud::scheduler
