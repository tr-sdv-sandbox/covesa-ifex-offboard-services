#pragma once

#include <memory>
#include <optional>
#include <string>

#include "dispatcher-rpc-envelope.pb.h"
#include "postgres_client.hpp"
#include "rpc-offboard.pb.h"

namespace ifex::offboard {

/// RPC request record from database
struct RpcRequestRecord {
    std::string correlation_id;
    std::string vehicle_id;
    std::string service_name;
    std::string method_name;
    std::string parameters_json;
    uint32_t timeout_ms;
    std::string response_status;
    std::string result_json;
    std::string error_message;
    bool responded;
};

/// PostgreSQL store for RPC request/response tracking
class RpcStore {
public:
    explicit RpcStore(std::shared_ptr<PostgresClient> db);

    /// Ensure vehicle exists in database with optional enrichment
    void upsert_vehicle(const std::string& vehicle_id,
                        const std::string& fleet_id = "",
                        const std::string& region = "");

    /// Process an offboard RPC message (response from Kafka)
    void process_offboard_message(const rpc::rpc_offboard_t& msg);

    /// Create a new RPC request record (before sending to vehicle)
    void create_request(
        const std::string& vehicle_id,
        const swdv::dispatcher_rpc_envelope::rpc_request_t& request);

    /// Update request with response
    void record_response(
        const swdv::dispatcher_rpc_envelope::rpc_response_t& response);

    /// Get pending request by correlation_id
    std::optional<RpcRequestRecord> get_request(
        const std::string& correlation_id);

    /// Get all pending requests for a vehicle
    std::vector<RpcRequestRecord> get_pending_requests(
        const std::string& vehicle_id);

    /// Mark timed-out requests
    int mark_timed_out_requests(int64_t timeout_threshold_ms);

private:
    std::shared_ptr<PostgresClient> db_;
};

}  // namespace ifex::offboard
