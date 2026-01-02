#include "rpc_codec.hpp"

#include <chrono>

#include <glog/logging.h>

namespace ifex::offboard {

std::optional<swdv::dispatcher_rpc_envelope::rpc_request_t>
decode_rpc_request(const std::string& payload) {
    swdv::dispatcher_rpc_envelope::rpc_request_t msg;
    if (!msg.ParseFromString(payload)) {
        LOG(WARNING) << "Failed to parse RPC request";
        return std::nullopt;
    }
    return msg;
}

std::optional<swdv::dispatcher_rpc_envelope::rpc_response_t>
decode_rpc_response(const std::string& payload) {
    swdv::dispatcher_rpc_envelope::rpc_response_t msg;
    if (!msg.ParseFromString(payload)) {
        LOG(WARNING) << "Failed to parse RPC response";
        return std::nullopt;
    }
    return msg;
}

std::string encode_rpc_request(
    const swdv::dispatcher_rpc_envelope::rpc_request_t& req) {
    std::string output;
    req.SerializeToString(&output);
    return output;
}

std::string encode_rpc_response(
    const swdv::dispatcher_rpc_envelope::rpc_response_t& resp) {
    std::string output;
    resp.SerializeToString(&output);
    return output;
}

swdv::dispatcher_rpc_envelope::rpc_request_t create_rpc_request(
    const std::string& correlation_id,
    const std::string& service_name,
    const std::string& method_name,
    const std::string& parameters_json,
    uint32_t timeout_ms) {
    swdv::dispatcher_rpc_envelope::rpc_request_t req;
    req.set_correlation_id(correlation_id);
    req.set_service_name(service_name);
    req.set_method_name(method_name);
    req.set_parameters_json(parameters_json);
    req.set_timeout_ms(timeout_ms);

    auto now = std::chrono::system_clock::now();
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        now.time_since_epoch()).count();
    req.set_request_timestamp_ns(ns);

    return req;
}

const char* rpc_status_name(
    swdv::dispatcher_rpc_envelope::rpc_status_t status) {
    switch (status) {
        case swdv::dispatcher_rpc_envelope::SUCCESS:
            return "SUCCESS";
        case swdv::dispatcher_rpc_envelope::FAILED:
            return "FAILED";
        case swdv::dispatcher_rpc_envelope::TIMEOUT:
            return "TIMEOUT";
        case swdv::dispatcher_rpc_envelope::SERVICE_UNAVAILABLE:
            return "SERVICE_UNAVAILABLE";
        case swdv::dispatcher_rpc_envelope::METHOD_NOT_FOUND:
            return "METHOD_NOT_FOUND";
        case swdv::dispatcher_rpc_envelope::INVALID_PARAMETERS:
            return "INVALID_PARAMETERS";
        case swdv::dispatcher_rpc_envelope::TRANSPORT_ERROR:
            return "TRANSPORT_ERROR";
        case swdv::dispatcher_rpc_envelope::DUPLICATE_REQUEST:
            return "DUPLICATE_REQUEST";
        default:
            return "UNKNOWN";
    }
}

}  // namespace ifex::offboard
