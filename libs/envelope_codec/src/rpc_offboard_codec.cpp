#include "rpc_offboard_codec.hpp"

#include <chrono>
#include <glog/logging.h>

namespace ifex::offboard {

namespace {

uint64_t now_ns() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        now.time_since_epoch()).count();
}

std::string rpc_status_to_string(
    swdv::dispatcher_rpc_envelope::rpc_status_t status) {
    switch (status) {
        case swdv::dispatcher_rpc_envelope::SUCCESS: return "SUCCESS";
        case swdv::dispatcher_rpc_envelope::FAILED: return "FAILED";
        case swdv::dispatcher_rpc_envelope::TIMEOUT: return "TIMEOUT";
        case swdv::dispatcher_rpc_envelope::SERVICE_UNAVAILABLE: return "SERVICE_UNAVAILABLE";
        case swdv::dispatcher_rpc_envelope::METHOD_NOT_FOUND: return "METHOD_NOT_FOUND";
        case swdv::dispatcher_rpc_envelope::INVALID_PARAMETERS: return "INVALID_PARAMETERS";
        case swdv::dispatcher_rpc_envelope::TRANSPORT_ERROR: return "TRANSPORT_ERROR";
        case swdv::dispatcher_rpc_envelope::DUPLICATE_REQUEST: return "DUPLICATE_REQUEST";
        default: return "UNKNOWN";
    }
}

}  // namespace

std::string encode_rpc_response_offboard(
    const std::string& vehicle_id,
    const swdv::dispatcher_rpc_envelope::rpc_response_t& response,
    const std::string& fleet_id,
    const std::string& region,
    const std::string& bridge_id) {

    rpc::rpc_offboard_t offboard;

    // Set metadata
    auto* meta = offboard.mutable_metadata();
    meta->set_vehicle_id(vehicle_id);
    meta->set_fleet_id(fleet_id);
    meta->set_region(region);
    meta->set_received_at_ns(now_ns());
    meta->set_bridge_id(bridge_id);

    // Set direction
    offboard.set_direction(rpc::VEHICLE_TO_CLOUD);

    // Copy the response
    *offboard.mutable_response() = response;

    return offboard.SerializeAsString();
}

std::string encode_rpc_request_offboard(
    const std::string& vehicle_id,
    const swdv::dispatcher_rpc_envelope::rpc_request_t& request,
    const std::string& request_id,
    const std::string& requester_id,
    uint64_t timeout_at_ns,
    const std::string& fleet_id,
    const std::string& region,
    const std::string& bridge_id) {

    rpc::rpc_offboard_t offboard;

    // Set metadata
    auto* meta = offboard.mutable_metadata();
    meta->set_vehicle_id(vehicle_id);
    meta->set_fleet_id(fleet_id);
    meta->set_region(region);
    meta->set_received_at_ns(now_ns());
    meta->set_bridge_id(bridge_id);

    // Set direction
    offboard.set_direction(rpc::CLOUD_TO_VEHICLE);

    // Copy the request
    *offboard.mutable_request() = request;

    // Cloud-side tracking
    offboard.set_request_id(request_id);

    return offboard.SerializeAsString();
}

std::optional<rpc::rpc_offboard_t> decode_rpc_offboard(const std::string& data) {
    rpc::rpc_offboard_t msg;
    if (msg.ParseFromString(data)) {
        return msg;
    }
    return std::nullopt;
}

rpc::rpc_record_t create_rpc_record_from_request(
    const std::string& vehicle_id,
    const swdv::dispatcher_rpc_envelope::rpc_request_t& request,
    const std::string& request_id,
    const std::string& requester_id,
    const std::string& fleet_id,
    const std::string& region) {

    rpc::rpc_record_t record;

    // Identity
    record.set_vehicle_id(vehicle_id);
    record.set_correlation_id(request.correlation_id());
    record.set_request_id(request_id);

    // Request info
    record.set_service_name(request.service_name());
    record.set_method_name(request.method_name());
    record.set_parameters_json(request.parameters_json());
    record.set_timeout_ms(request.timeout_ms());

    // Timing
    record.set_request_timestamp_ns(request.request_timestamp_ns());

    // Enrichment
    record.set_fleet_id(fleet_id);
    record.set_region(region);
    record.set_requester_id(requester_id);

    return record;
}

void update_rpc_record_with_response(
    rpc::rpc_record_t& record,
    const swdv::dispatcher_rpc_envelope::rpc_response_t& response) {

    // Response info
    record.set_status(rpc_status_to_string(response.status()));
    record.set_result_json(response.result_json());
    record.set_error_message(response.error_message());

    // Timing
    uint64_t response_ts = now_ns();
    record.set_response_timestamp_ns(response_ts);

    if (record.request_timestamp_ns() > 0) {
        // Convert request timestamp from vehicle ns to compare
        // Note: This assumes reasonable clock sync; in production
        // you'd use the offboard received_at_ns instead
        record.set_round_trip_ns(response_ts - record.request_timestamp_ns());
    }
}

}  // namespace ifex::offboard
