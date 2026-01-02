#pragma once

#include <optional>
#include <string>

#include "dispatcher-rpc-envelope.pb.h"

namespace ifex::offboard {

/// Decode RPC request from binary payload
std::optional<swdv::dispatcher_rpc_envelope::rpc_request_t>
decode_rpc_request(const std::string& payload);

/// Decode RPC response from binary payload
std::optional<swdv::dispatcher_rpc_envelope::rpc_response_t>
decode_rpc_response(const std::string& payload);

/// Encode RPC request to binary
std::string encode_rpc_request(
    const swdv::dispatcher_rpc_envelope::rpc_request_t& req);

/// Encode RPC response to binary
std::string encode_rpc_response(
    const swdv::dispatcher_rpc_envelope::rpc_response_t& resp);

/// Create RPC request
swdv::dispatcher_rpc_envelope::rpc_request_t create_rpc_request(
    const std::string& correlation_id,
    const std::string& service_name,
    const std::string& method_name,
    const std::string& parameters_json,
    uint32_t timeout_ms);

/// Helper to get RPC status name
const char* rpc_status_name(
    swdv::dispatcher_rpc_envelope::rpc_status_t status);

}  // namespace ifex::offboard
