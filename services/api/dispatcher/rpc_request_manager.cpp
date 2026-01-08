#include "rpc_request_manager.hpp"

#include <glog/logging.h>
#include <random>
#include <sstream>
#include <iomanip>

namespace ifex {
namespace cloud {
namespace dispatcher {

const char* rpc_status_to_string(RpcStatus status) {
    switch (status) {
        case RpcStatus::PENDING: return "PENDING";
        case RpcStatus::IN_PROGRESS: return "IN_PROGRESS";
        case RpcStatus::SUCCESS: return "SUCCESS";
        case RpcStatus::TIMEOUT: return "TIMEOUT";
        case RpcStatus::ERROR: return "ERROR";
        case RpcStatus::CANCELLED: return "CANCELLED";
        default: return "UNKNOWN";
    }
}

RpcStatus rpc_status_from_int(int value) {
    if (value >= 0 && value <= 5) {
        return static_cast<RpcStatus>(value);
    }
    return RpcStatus::ERROR;
}

RpcRequestManager::RpcRequestManager(Config config)
    : config_(std::move(config)) {
    // Start cleanup thread
    cleanup_thread_ = std::thread([this]() { cleanup_thread_func(); });
    LOG(INFO) << "RpcRequestManager started with pending_timeout="
              << config_.pending_timeout.count() << "s, completed_ttl="
              << config_.completed_ttl.count() << "s";
}

RpcRequestManager::~RpcRequestManager() {
    running_ = false;
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    LOG(INFO) << "RpcRequestManager stopped. Stats: created=" << stats_.requests_created
              << " received=" << stats_.responses_received
              << " matched=" << stats_.responses_matched
              << " orphaned=" << stats_.responses_orphaned
              << " timed_out=" << stats_.requests_timed_out;
}

std::string RpcRequestManager::generate_correlation_id() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dist;

    std::stringstream ss;
    ss << std::hex << std::setfill('0');
    ss << std::setw(16) << dist(gen);
    ss << std::setw(16) << dist(gen);
    return ss.str();
}

std::string RpcRequestManager::create_request(
    const std::string& vehicle_id,
    const std::string& service_name,
    const std::string& method_name,
    const std::string& parameters_json,
    const std::string& requester_id,
    std::chrono::milliseconds timeout) {

    std::string correlation_id = generate_correlation_id();
    auto now = std::chrono::steady_clock::now();

    // Use provided timeout or default
    auto actual_timeout = timeout.count() > 0
        ? timeout
        : std::chrono::duration_cast<std::chrono::milliseconds>(config_.pending_timeout);

    PendingRequest request;
    request.correlation_id = correlation_id;
    request.vehicle_id = vehicle_id;
    request.service_name = service_name;
    request.method_name = method_name;
    request.requester_id = requester_id;
    request.created_at = now;
    request.deadline = now + actual_timeout;
    request.status = RpcStatus::PENDING;

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_requests_[correlation_id] = std::move(request);
    }

    stats_.requests_created++;

    VLOG(1) << "Created RPC request " << correlation_id
            << " vehicle=" << vehicle_id
            << " method=" << service_name << "." << method_name
            << " timeout=" << actual_timeout.count() << "ms";

    return correlation_id;
}

void RpcRequestManager::on_response(const RpcResponse& response) {
    stats_.responses_received++;

    VLOG(1) << "Received RPC response: correlation_id=" << response.correlation_id
            << " status=" << rpc_status_to_string(response.status);

    // Find and remove from pending
    PendingRequest pending;
    bool found = false;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_requests_.find(response.correlation_id);
        if (it != pending_requests_.end()) {
            pending = std::move(it->second);
            pending_requests_.erase(it);
            found = true;
        }
    }

    if (!found) {
        // Response for unknown/expired request
        stats_.responses_orphaned++;
        LOG(WARNING) << "Response for unknown request: " << response.correlation_id;
        return;
    }

    stats_.responses_matched++;

    // Create completed response
    auto now = std::chrono::steady_clock::now();
    CompletedResponse completed;
    completed.correlation_id = response.correlation_id;
    completed.vehicle_id = pending.vehicle_id;
    completed.service_name = pending.service_name;
    completed.method_name = pending.method_name;
    completed.requester_id = pending.requester_id;
    completed.status = response.status;
    completed.result_json = response.result_json;
    completed.error_message = response.error_message;
    completed.execution_time_ms = response.execution_time_ms;
    completed.created_at = pending.created_at;
    completed.completed_at = now;
    completed.expires_at = now + config_.completed_ttl;

    // Store in completed map
    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        completed_responses_[response.correlation_id] = std::move(completed);
    }

    LOG(INFO) << "RPC completed: correlation_id=" << response.correlation_id
              << " status=" << rpc_status_to_string(response.status)
              << " duration=" << response.execution_time_ms << "ms";
}

bool RpcRequestManager::cancel_request(const std::string& correlation_id) {
    // Remove from pending
    PendingRequest pending;
    bool found = false;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_requests_.find(correlation_id);
        if (it != pending_requests_.end()) {
            pending = std::move(it->second);
            pending_requests_.erase(it);
            found = true;
        }
    }

    if (!found) {
        return false;
    }

    stats_.requests_cancelled++;

    // Create cancelled completion record
    auto now = std::chrono::steady_clock::now();
    CompletedResponse completed;
    completed.correlation_id = correlation_id;
    completed.vehicle_id = pending.vehicle_id;
    completed.service_name = pending.service_name;
    completed.method_name = pending.method_name;
    completed.requester_id = pending.requester_id;
    completed.status = RpcStatus::CANCELLED;
    completed.error_message = "Request cancelled by user";
    completed.created_at = pending.created_at;
    completed.completed_at = now;
    completed.expires_at = now + config_.completed_ttl;

    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        completed_responses_[correlation_id] = std::move(completed);
    }

    LOG(INFO) << "Cancelled RPC request: " << correlation_id;
    return true;
}

std::optional<RpcRequestInfo> RpcRequestManager::get_request(const std::string& correlation_id) {
    // Check completed first (most common case for polling)
    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        auto it = completed_responses_.find(correlation_id);
        if (it != completed_responses_.end()) {
            const auto& resp = it->second;
            RpcRequestInfo info;
            info.correlation_id = resp.correlation_id;
            info.vehicle_id = resp.vehicle_id;
            info.service_name = resp.service_name;
            info.method_name = resp.method_name;
            info.requester_id = resp.requester_id;
            info.status = resp.status;
            info.created_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                resp.created_at.time_since_epoch()).count();
            info.completed_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                resp.completed_at.time_since_epoch()).count();
            info.result_json = resp.result_json;
            info.error_message = resp.error_message;
            info.execution_time_ms = resp.execution_time_ms;
            return info;
        }
    }

    // Check pending
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_requests_.find(correlation_id);
        if (it != pending_requests_.end()) {
            const auto& req = it->second;
            RpcRequestInfo info;
            info.correlation_id = req.correlation_id;
            info.vehicle_id = req.vehicle_id;
            info.service_name = req.service_name;
            info.method_name = req.method_name;
            info.requester_id = req.requester_id;
            info.status = req.status;
            info.created_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                req.created_at.time_since_epoch()).count();
            info.completed_at_ms = 0;
            info.result_json = "";
            info.error_message = "";
            info.execution_time_ms = 0;
            return info;
        }
    }

    return std::nullopt;
}

std::vector<RpcRequestInfo> RpcRequestManager::list_requests(
    const std::string& vehicle_id_filter,
    const std::string& service_filter,
    RpcStatus status_filter,
    int limit,
    int offset) {

    std::vector<RpcRequestInfo> results;
    int skipped = 0;

    auto matches_filters = [&](const std::string& vehicle_id,
                               const std::string& service_name,
                               RpcStatus status) {
        if (!vehicle_id_filter.empty() && vehicle_id != vehicle_id_filter) {
            return false;
        }
        if (!service_filter.empty() && service_name != service_filter) {
            return false;
        }
        // status_filter: PENDING means "all" for backwards compatibility
        if (status_filter != RpcStatus::PENDING && status != status_filter) {
            return false;
        }
        return true;
    };

    // Add pending requests
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (const auto& [corr_id, req] : pending_requests_) {
            if (!matches_filters(req.vehicle_id, req.service_name, req.status)) {
                continue;
            }
            if (skipped < offset) {
                ++skipped;
                continue;
            }
            if (static_cast<int>(results.size()) >= limit) {
                break;
            }

            RpcRequestInfo info;
            info.correlation_id = req.correlation_id;
            info.vehicle_id = req.vehicle_id;
            info.service_name = req.service_name;
            info.method_name = req.method_name;
            info.requester_id = req.requester_id;
            info.status = req.status;
            info.created_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                req.created_at.time_since_epoch()).count();
            info.completed_at_ms = 0;
            info.result_json = "";
            info.error_message = "";
            info.execution_time_ms = 0;
            results.push_back(std::move(info));
        }
    }

    // Add completed responses (if not at limit)
    if (static_cast<int>(results.size()) < limit) {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        for (const auto& [corr_id, resp] : completed_responses_) {
            if (!matches_filters(resp.vehicle_id, resp.service_name, resp.status)) {
                continue;
            }
            if (skipped < offset) {
                ++skipped;
                continue;
            }
            if (static_cast<int>(results.size()) >= limit) {
                break;
            }

            RpcRequestInfo info;
            info.correlation_id = resp.correlation_id;
            info.vehicle_id = resp.vehicle_id;
            info.service_name = resp.service_name;
            info.method_name = resp.method_name;
            info.requester_id = resp.requester_id;
            info.status = resp.status;
            info.created_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                resp.created_at.time_since_epoch()).count();
            info.completed_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                resp.completed_at.time_since_epoch()).count();
            info.result_json = resp.result_json;
            info.error_message = resp.error_message;
            info.execution_time_ms = resp.execution_time_ms;
            results.push_back(std::move(info));
        }
    }

    return results;
}

size_t RpcRequestManager::pending_count() const {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    return pending_requests_.size();
}

size_t RpcRequestManager::completed_count() const {
    std::lock_guard<std::mutex> lock(completed_mutex_);
    return completed_responses_.size();
}

void RpcRequestManager::cleanup_thread_func() {
    while (running_) {
        std::this_thread::sleep_for(config_.cleanup_interval);
        if (running_) {
            cleanup_expired_pending();
            cleanup_expired_completed();
        }
    }
}

void RpcRequestManager::cleanup_expired_pending() {
    auto now = std::chrono::steady_clock::now();
    std::vector<PendingRequest> expired;

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (auto it = pending_requests_.begin(); it != pending_requests_.end(); ) {
            if (it->second.deadline < now) {
                expired.push_back(std::move(it->second));
                it = pending_requests_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Move expired to completed with TIMEOUT status
    if (!expired.empty()) {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        for (auto& req : expired) {
            stats_.requests_timed_out++;

            CompletedResponse completed;
            completed.correlation_id = req.correlation_id;
            completed.vehicle_id = req.vehicle_id;
            completed.service_name = req.service_name;
            completed.method_name = req.method_name;
            completed.requester_id = req.requester_id;
            completed.status = RpcStatus::TIMEOUT;
            completed.error_message = "Request timed out waiting for vehicle response";
            completed.created_at = req.created_at;
            completed.completed_at = now;
            completed.expires_at = now + config_.completed_ttl;

            completed_responses_[req.correlation_id] = std::move(completed);

            LOG(WARNING) << "RPC request timed out: " << req.correlation_id
                         << " vehicle=" << req.vehicle_id;
        }
    }
}

void RpcRequestManager::cleanup_expired_completed() {
    auto now = std::chrono::steady_clock::now();
    size_t evicted = 0;

    {
        std::lock_guard<std::mutex> lock(completed_mutex_);
        for (auto it = completed_responses_.begin(); it != completed_responses_.end(); ) {
            if (it->second.expires_at < now) {
                it = completed_responses_.erase(it);
                evicted++;
            } else {
                ++it;
            }
        }
    }

    if (evicted > 0) {
        stats_.completed_evicted += evicted;
        VLOG(1) << "Evicted " << evicted << " expired completed responses";
    }
}

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
