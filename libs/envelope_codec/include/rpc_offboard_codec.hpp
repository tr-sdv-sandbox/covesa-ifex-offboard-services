#pragma once

#include <memory>
#include <optional>
#include <string>

#include "rpc-offboard.pb.h"
#include "dispatcher-rpc-envelope.pb.h"

namespace ifex::offboard {

/// Create an offboard RPC response message (v2c direction)
std::string encode_rpc_response_offboard(
    const std::string& vehicle_id,
    const swdv::dispatcher_rpc_envelope::rpc_response_t& response,
    const std::string& fleet_id = "",
    const std::string& region = "",
    const std::string& bridge_id = "");

/// Create an offboard RPC request message (c2v direction)
std::string encode_rpc_request_offboard(
    const std::string& vehicle_id,
    const swdv::dispatcher_rpc_envelope::rpc_request_t& request,
    const std::string& request_id,
    const std::string& requester_id,
    uint64_t timeout_at_ns,
    const std::string& fleet_id = "",
    const std::string& region = "",
    const std::string& bridge_id = "");

/// Decode an offboard RPC message (can be request or response)
std::optional<rpc::rpc_offboard_t> decode_rpc_offboard(const std::string& data);

/// Create a flattened RPC record for PostgreSQL
rpc::rpc_record_t create_rpc_record_from_request(
    const std::string& vehicle_id,
    const swdv::dispatcher_rpc_envelope::rpc_request_t& request,
    const std::string& request_id,
    const std::string& requester_id,
    const std::string& fleet_id = "",
    const std::string& region = "");

/// Update RPC record with response data
void update_rpc_record_with_response(
    rpc::rpc_record_t& record,
    const swdv::dispatcher_rpc_envelope::rpc_response_t& response);

}  // namespace ifex::offboard
