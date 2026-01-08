#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

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

/// Pending RPC request (in-memory tracking)
struct PendingRequest {
    std::string correlation_id;
    std::string vehicle_id;
    std::string service_name;
    std::string method_name;
    std::string requester_id;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point deadline;
    RpcStatus status = RpcStatus::PENDING;
};

/// Completed RPC response (stored for retrieval)
struct CompletedResponse {
    std::string correlation_id;
    std::string vehicle_id;
    std::string service_name;
    std::string method_name;
    std::string requester_id;
    RpcStatus status;
    std::string result_json;
    std::string error_message;
    int64_t execution_time_ms = 0;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point completed_at;
    std::chrono::steady_clock::time_point expires_at;
};

/// RPC response from vehicle (incoming)
struct RpcResponse {
    std::string correlation_id;
    std::string vehicle_id;
    RpcStatus status;
    std::string result_json;
    std::string error_message;
    int64_t execution_time_ms;
};

/// RPC request info for listing (returned by get_request/list_requests)
struct RpcRequestInfo {
    std::string correlation_id;
    std::string vehicle_id;
    std::string service_name;
    std::string method_name;
    std::string requester_id;
    RpcStatus status;
    int64_t created_at_ms;
    int64_t completed_at_ms;
    std::string result_json;
    std::string error_message;
    int64_t execution_time_ms;
};

/// Statistics for monitoring
struct RpcManagerStats {
    std::atomic<uint64_t> requests_created{0};
    std::atomic<uint64_t> responses_received{0};
    std::atomic<uint64_t> responses_matched{0};
    std::atomic<uint64_t> responses_orphaned{0};  // response for unknown request
    std::atomic<uint64_t> requests_timed_out{0};
    std::atomic<uint64_t> requests_cancelled{0};
    std::atomic<uint64_t> completed_evicted{0};   // evicted from completed cache
};

/// RpcRequestManager configuration
struct RpcManagerConfig {
    std::chrono::seconds pending_timeout{30};      // Max time to wait for response
    std::chrono::seconds completed_ttl{300};       // Keep completed responses for 5 min
    std::chrono::seconds cleanup_interval{5};      // Cleanup thread interval
};

/**
 * Manages RPC request correlation and tracking (in-memory only)
 *
 * Async-only design for scalability (100K+ concurrent requests):
 * - create_request() returns correlation_id immediately
 * - on_response() stores response in completed_responses_ map
 * - get_request() retrieves status/result by correlation_id
 * - Cleanup thread handles timeouts and TTL eviction
 *
 * No blocking waits - all operations are non-blocking.
 * RPCs are ephemeral - no database persistence.
 */
class RpcRequestManager {
public:
    using Config = RpcManagerConfig;

    explicit RpcRequestManager(Config config = {});
    ~RpcRequestManager();

    /**
     * Create a new RPC request (async - returns immediately)
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
     * Handle incoming response from vehicle
     * Called by response consumer - stores in completed_responses_
     */
    void on_response(const RpcResponse& response);

    /**
     * Cancel a pending request
     */
    bool cancel_request(const std::string& correlation_id);

    /**
     * Get request status/result (checks pending then completed)
     */
    std::optional<RpcRequestInfo> get_request(const std::string& correlation_id);

    /**
     * List requests (both pending and completed)
     */
    std::vector<RpcRequestInfo> list_requests(
        const std::string& vehicle_id_filter,
        const std::string& service_filter,
        RpcStatus status_filter,
        int limit,
        int offset);

    /**
     * Get statistics
     */
    const RpcManagerStats& stats() const { return stats_; }

    /**
     * Get current counts
     */
    size_t pending_count() const;
    size_t completed_count() const;

private:
    std::string generate_correlation_id();
    void cleanup_thread_func();
    void cleanup_expired_pending();
    void cleanup_expired_completed();

    Config config_;

    // Pending requests (waiting for response)
    mutable std::mutex pending_mutex_;
    std::map<std::string, PendingRequest> pending_requests_;

    // Completed responses (queryable with TTL)
    mutable std::mutex completed_mutex_;
    std::map<std::string, CompletedResponse> completed_responses_;

    // Statistics
    RpcManagerStats stats_;

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
