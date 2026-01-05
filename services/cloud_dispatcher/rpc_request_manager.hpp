#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "postgres_client.hpp"

namespace ifex {
namespace cloud {
namespace dispatcher {

/// RPC request status
enum class RpcStatus {
    PENDING = 0,
    IN_PROGRESS = 1,
    SUCCESS = 2,
    TIMEOUT = 3,
    ERROR = 4,
    CANCELLED = 5
};

/// Pending RPC request
struct PendingRequest {
    std::string correlation_id;
    std::string vehicle_id;
    std::string service_name;
    std::string method_name;
    std::chrono::steady_clock::time_point deadline;
    std::condition_variable cv;
    std::mutex mutex;
    bool completed = false;
    RpcStatus status = RpcStatus::PENDING;
    std::string result_json;
    std::string error_message;
};

/// RPC response from vehicle
struct RpcResponse {
    std::string correlation_id;
    std::string vehicle_id;
    RpcStatus status;
    std::string result_json;
    std::string error_message;
    int64_t execution_time_ms;
};

/// RPC request info for listing
struct RpcRequestInfo {
    std::string correlation_id;
    std::string vehicle_id;
    std::string service_name;
    std::string method_name;
    std::string requester_id;
    RpcStatus status;
    int64_t created_at_ns;
    int64_t completed_at_ns;
    std::string result_json;
    std::string error_message;
};

/**
 * Manages RPC request correlation and tracking
 *
 * - Tracks pending requests by correlation_id
 * - Handles timeouts
 * - Stores request/response in PostgreSQL
 * - Provides synchronous wait for response
 */
class RpcRequestManager {
public:
    explicit RpcRequestManager(std::shared_ptr<ifex::offboard::PostgresClient> db);
    ~RpcRequestManager();

    /**
     * Create a new RPC request
     * @return correlation_id for tracking
     */
    std::string create_request(
        const std::string& vehicle_id,
        const std::string& service_name,
        const std::string& method_name,
        const std::string& parameters_json,
        const std::string& requester_id,
        std::chrono::milliseconds timeout);

    /**
     * Wait for a response (synchronous call)
     * @return true if response received, false if timeout
     */
    bool wait_for_response(
        const std::string& correlation_id,
        std::chrono::milliseconds timeout,
        RpcResponse& response);

    /**
     * Handle incoming response from vehicle
     * Called by response consumer
     */
    void on_response(const RpcResponse& response);

    /**
     * Cancel a pending request
     */
    bool cancel_request(const std::string& correlation_id);

    /**
     * Get request status
     */
    std::optional<RpcRequestInfo> get_request(const std::string& correlation_id);

    /**
     * List recent requests
     */
    std::vector<RpcRequestInfo> list_requests(
        const std::string& vehicle_id_filter,
        const std::string& service_filter,
        RpcStatus status_filter,
        int limit,
        int offset);

    /**
     * Clean up expired pending requests
     * Called periodically by cleanup thread
     */
    void cleanup_expired();

private:
    std::string generate_correlation_id();
    void store_request(const std::string& correlation_id,
                       const std::string& vehicle_id,
                       const std::string& service_name,
                       const std::string& method_name,
                       const std::string& parameters_json,
                       const std::string& requester_id);
    void update_request_status(const std::string& correlation_id,
                                RpcStatus status,
                                const std::string& result_json = "",
                                const std::string& error_message = "");

    std::shared_ptr<ifex::offboard::PostgresClient> db_;

    // Pending requests (in-memory for fast lookup)
    std::mutex pending_mutex_;
    std::map<std::string, std::shared_ptr<PendingRequest>> pending_requests_;

    // Cleanup thread
    std::atomic<bool> running_{true};
    std::thread cleanup_thread_;
};

// Helper to convert status to string
const char* rpc_status_to_string(RpcStatus status);
RpcStatus rpc_status_from_int(int value);

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
