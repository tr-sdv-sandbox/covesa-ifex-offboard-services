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

RpcRequestManager::RpcRequestManager(std::shared_ptr<ifex::offboard::PostgresClient> db)
    : db_(std::move(db)) {

    // Start cleanup thread
    cleanup_thread_ = std::thread([this]() {
        while (running_) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            if (running_) {
                cleanup_expired();
            }
        }
    });
}

RpcRequestManager::~RpcRequestManager() {
    running_ = false;
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
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

    // Create pending request
    auto request = std::make_shared<PendingRequest>();
    request->correlation_id = correlation_id;
    request->vehicle_id = vehicle_id;
    request->service_name = service_name;
    request->method_name = method_name;
    request->deadline = std::chrono::steady_clock::now() + timeout;

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_requests_[correlation_id] = request;
    }

    // Store in database
    store_request(correlation_id, vehicle_id, service_name, method_name,
                  parameters_json, requester_id);

    LOG(INFO) << "Created RPC request " << correlation_id
              << " vehicle=" << vehicle_id
              << " method=" << service_name << "." << method_name;

    return correlation_id;
}

bool RpcRequestManager::wait_for_response(
    const std::string& correlation_id,
    std::chrono::milliseconds timeout,
    RpcResponse& response) {

    std::shared_ptr<PendingRequest> request;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_requests_.find(correlation_id);
        if (it == pending_requests_.end()) {
            LOG(WARNING) << "Request not found: " << correlation_id;
            return false;
        }
        request = it->second;
    }

    // Wait for response or timeout
    std::unique_lock<std::mutex> lock(request->mutex);
    bool completed = request->cv.wait_for(lock, timeout, [&request]() {
        return request->completed;
    });

    if (!completed) {
        // Timeout
        request->status = RpcStatus::TIMEOUT;
        request->error_message = "Request timed out";
        update_request_status(correlation_id, RpcStatus::TIMEOUT, "", "Request timed out");
    }

    // Build response
    response.correlation_id = correlation_id;
    response.vehicle_id = request->vehicle_id;
    response.status = request->status;
    response.result_json = request->result_json;
    response.error_message = request->error_message;

    // Clean up pending request
    {
        std::lock_guard<std::mutex> plock(pending_mutex_);
        pending_requests_.erase(correlation_id);
    }

    return completed && request->status == RpcStatus::SUCCESS;
}

void RpcRequestManager::on_response(const RpcResponse& response) {
    LOG(INFO) << "Received RPC response: correlation_id=" << response.correlation_id
              << " status=" << rpc_status_to_string(response.status);

    std::shared_ptr<PendingRequest> request;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_requests_.find(response.correlation_id);
        if (it == pending_requests_.end()) {
            LOG(WARNING) << "Response for unknown request: " << response.correlation_id;
            return;
        }
        request = it->second;
    }

    // Update request
    {
        std::lock_guard<std::mutex> lock(request->mutex);
        request->completed = true;
        request->status = response.status;
        request->result_json = response.result_json;
        request->error_message = response.error_message;
    }

    // Update database
    update_request_status(response.correlation_id, response.status,
                          response.result_json, response.error_message);

    // Notify waiting thread
    request->cv.notify_one();
}

bool RpcRequestManager::cancel_request(const std::string& correlation_id) {
    std::shared_ptr<PendingRequest> request;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        auto it = pending_requests_.find(correlation_id);
        if (it == pending_requests_.end()) {
            return false;
        }
        request = it->second;
    }

    // Update request
    {
        std::lock_guard<std::mutex> lock(request->mutex);
        request->completed = true;
        request->status = RpcStatus::CANCELLED;
        request->error_message = "Request cancelled by user";
    }

    // Update database
    update_request_status(correlation_id, RpcStatus::CANCELLED, "", "Request cancelled by user");

    // Notify waiting thread
    request->cv.notify_one();

    LOG(INFO) << "Cancelled RPC request: " << correlation_id;
    return true;
}

std::optional<RpcRequestInfo> RpcRequestManager::get_request(const std::string& correlation_id) {
    const char* sql = R"(
        SELECT correlation_id, vehicle_id, service_name, method_name,
               requester_id, status, created_at_ns, completed_at_ns,
               result_json, error_message
        FROM rpc_requests
        WHERE correlation_id = $1
    )";

    auto result = db_->execute(sql, {correlation_id});
    if (result.num_rows() == 0) {
        return std::nullopt;
    }

    auto row = result.row(0);
    RpcRequestInfo info;
    info.correlation_id = row.get_string(0);
    info.vehicle_id = row.get_string(1);
    info.service_name = row.get_string(2);
    info.method_name = row.get_string(3);
    info.requester_id = row.get_string(4);
    info.status = rpc_status_from_int(row.get_int(5));
    info.created_at_ns = row.get_int64(6);
    info.completed_at_ns = row.get_int64(7);
    info.result_json = row.get_string(8);
    info.error_message = row.get_string(9);

    return info;
}

std::vector<RpcRequestInfo> RpcRequestManager::list_requests(
    const std::string& vehicle_id_filter,
    const std::string& service_filter,
    RpcStatus status_filter,
    int limit,
    int offset) {

    std::string sql = R"(
        SELECT correlation_id, vehicle_id, service_name, method_name,
               requester_id, status, created_at_ns, completed_at_ns,
               result_json, error_message
        FROM rpc_requests
        WHERE 1=1
    )";

    std::vector<std::string> params;
    int param_idx = 1;

    if (!vehicle_id_filter.empty()) {
        sql += " AND vehicle_id = $" + std::to_string(param_idx++);
        params.push_back(vehicle_id_filter);
    }
    if (!service_filter.empty()) {
        sql += " AND service_name = $" + std::to_string(param_idx++);
        params.push_back(service_filter);
    }
    if (status_filter != RpcStatus::PENDING || params.size() > 0) {
        // Only filter by status if explicitly specified
        if (static_cast<int>(status_filter) >= 0) {
            sql += " AND status = $" + std::to_string(param_idx++);
            params.push_back(std::to_string(static_cast<int>(status_filter)));
        }
    }

    sql += " ORDER BY created_at_ns DESC";
    sql += " LIMIT $" + std::to_string(param_idx++);
    params.push_back(std::to_string(limit));
    sql += " OFFSET $" + std::to_string(param_idx++);
    params.push_back(std::to_string(offset));

    auto result = db_->execute(sql, params);
    std::vector<RpcRequestInfo> requests;

    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);
        RpcRequestInfo info;
        info.correlation_id = row.get_string(0);
        info.vehicle_id = row.get_string(1);
        info.service_name = row.get_string(2);
        info.method_name = row.get_string(3);
        info.requester_id = row.get_string(4);
        info.status = rpc_status_from_int(row.get_int(5));
        info.created_at_ns = row.get_int64(6);
        info.completed_at_ns = row.get_int64(7);
        info.result_json = row.get_string(8);
        info.error_message = row.get_string(9);
        requests.push_back(std::move(info));
    }

    return requests;
}

void RpcRequestManager::cleanup_expired() {
    std::vector<std::string> expired_ids;
    auto now = std::chrono::steady_clock::now();

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        for (auto it = pending_requests_.begin(); it != pending_requests_.end(); ) {
            if (it->second->deadline < now) {
                expired_ids.push_back(it->first);
                it = pending_requests_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Update database for expired requests
    for (const auto& id : expired_ids) {
        update_request_status(id, RpcStatus::TIMEOUT, "", "Request expired");
        LOG(WARNING) << "RPC request expired: " << id;
    }
}

void RpcRequestManager::store_request(
    const std::string& correlation_id,
    const std::string& vehicle_id,
    const std::string& service_name,
    const std::string& method_name,
    const std::string& parameters_json,
    const std::string& requester_id) {

    const char* sql = R"(
        INSERT INTO rpc_requests
            (correlation_id, vehicle_id, service_name, method_name,
             parameters_json, requester_id, status, created_at_ns)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    )";

    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    db_->execute(sql, {
        correlation_id,
        vehicle_id,
        service_name,
        method_name,
        parameters_json,
        requester_id,
        std::to_string(static_cast<int>(RpcStatus::PENDING)),
        std::to_string(now_ns)
    });
}

void RpcRequestManager::update_request_status(
    const std::string& correlation_id,
    RpcStatus status,
    const std::string& result_json,
    const std::string& error_message) {

    const char* sql = R"(
        UPDATE rpc_requests
        SET status = $2,
            result_json = $3,
            error_message = $4,
            completed_at_ns = $5
        WHERE correlation_id = $1
    )";

    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    db_->execute(sql, {
        correlation_id,
        std::to_string(static_cast<int>(status)),
        result_json,
        error_message,
        std::to_string(now_ns)
    });
}

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
