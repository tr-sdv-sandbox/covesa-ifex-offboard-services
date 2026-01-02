#include "discovery_offboard_codec.hpp"

#include <chrono>
#include <glog/logging.h>

namespace ifex::offboard {

namespace {

uint64_t now_ns() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        now.time_since_epoch()).count();
}

std::string transport_type_to_string(
    swdv::discovery_sync_envelope::transport_type_t type) {
    switch (type) {
        case swdv::discovery_sync_envelope::GRPC: return "GRPC";
        case swdv::discovery_sync_envelope::HTTP_REST: return "HTTP_REST";
        case swdv::discovery_sync_envelope::DBUS: return "DBUS";
        case swdv::discovery_sync_envelope::SOMEIP: return "SOMEIP";
        case swdv::discovery_sync_envelope::MQTT: return "MQTT";
        default: return "UNKNOWN";
    }
}

std::string status_to_string(uint32_t status) {
    switch (status) {
        case 0: return "AVAILABLE";
        case 1: return "UNAVAILABLE";
        case 2: return "STARTING";
        case 3: return "STOPPING";
        case 4: return "ERROR";
        default: return "UNKNOWN";
    }
}

}  // namespace

std::string encode_discovery_offboard(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::sync_message_t& sync_msg,
    const std::string& fleet_id,
    const std::string& region,
    const std::string& bridge_id) {

    discovery::discovery_offboard_t offboard;

    // Set metadata
    auto* meta = offboard.mutable_metadata();
    meta->set_vehicle_id(vehicle_id);
    meta->set_fleet_id(fleet_id);
    meta->set_region(region);
    meta->set_received_at_ns(now_ns());
    meta->set_bridge_id(bridge_id);

    // Copy the original sync message
    *offboard.mutable_sync_message() = sync_msg;

    return offboard.SerializeAsString();
}

std::optional<discovery::discovery_offboard_t>
decode_discovery_offboard(const std::string& data) {
    discovery::discovery_offboard_t msg;
    if (msg.ParseFromString(data)) {
        return msg;
    }
    return std::nullopt;
}

discovery::service_record_t create_service_record(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::sync_event_t& event,
    const std::string& fleet_id,
    const std::string& region) {

    discovery::service_record_t record;

    record.set_vehicle_id(vehicle_id);
    record.set_sequence_number(event.sequence_number());

    if (event.has_service_info()) {
        const auto& svc = event.service_info();
        record.set_registration_id(svc.registration_id());
        record.set_service_name(svc.name());
        record.set_version(svc.version());
        record.set_description(svc.description());
        record.set_status(status_to_string(svc.status()));
        record.set_last_heartbeat_ms(svc.last_heartbeat_ms());

        if (svc.has_endpoint()) {
            record.set_endpoint_address(svc.endpoint().address());
            record.set_transport_type(transport_type_to_string(svc.endpoint().transport()));
        }
    } else {
        // For unregister events, just set the registration_id
        record.set_registration_id(event.registration_id());
    }

    // Enrichment
    record.set_fleet_id(fleet_id);
    record.set_region(region);

    // Timestamps
    auto ts = now_ns();
    record.set_first_seen_ns(ts);
    record.set_last_updated_ns(ts);

    return record;
}

}  // namespace ifex::offboard
