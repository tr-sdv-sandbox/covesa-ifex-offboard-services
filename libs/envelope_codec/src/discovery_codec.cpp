#include "discovery_codec.hpp"

#include <sstream>

#include <glog/logging.h>

namespace ifex::offboard {

std::optional<swdv::discovery_sync_envelope::sync_message_t>
decode_discovery_sync(const std::string& payload) {
    swdv::discovery_sync_envelope::sync_message_t msg;
    if (!msg.ParseFromString(payload)) {
        LOG(WARNING) << "Failed to parse discovery sync message";
        return std::nullopt;
    }
    return msg;
}

std::string encode_discovery_sync(
    const swdv::discovery_sync_envelope::sync_message_t& msg) {
    std::string output;
    msg.SerializeToString(&output);
    return output;
}

std::string encode_discovery_ack(
    uint64_t last_sequence,
    bool checksum_match,
    bool request_full_sync) {
    swdv::discovery_sync_envelope::sync_ack_t ack;
    ack.set_last_sequence_received(last_sequence);
    ack.set_checksum_match(checksum_match);
    ack.set_request_full_sync(request_full_sync);

    std::string output;
    ack.SerializeToString(&output);
    return output;
}

std::string service_info_to_string(
    const swdv::discovery_sync_envelope::service_info_t& info) {
    std::ostringstream ss;
    ss << "{"
       << "name=" << info.name()
       << ", version=" << info.version()
       << ", status=" << info.status()
       << ", registration_id=" << info.registration_id()
       << ", endpoint=" << info.endpoint().address()
       << "}";
    return ss.str();
}

const char* discovery_event_type_name(
    swdv::discovery_sync_envelope::sync_event_type_t type) {
    switch (type) {
        case swdv::discovery_sync_envelope::FULL_SYNC:
            return "FULL_SYNC";
        case swdv::discovery_sync_envelope::SERVICE_REGISTERED:
            return "SERVICE_REGISTERED";
        case swdv::discovery_sync_envelope::SERVICE_UNREGISTERED:
            return "SERVICE_UNREGISTERED";
        case swdv::discovery_sync_envelope::SERVICE_STATUS_CHANGED:
            return "SERVICE_STATUS_CHANGED";
        case swdv::discovery_sync_envelope::HEARTBEAT:
            return "HEARTBEAT";
        default:
            return "UNKNOWN";
    }
}

}  // namespace ifex::offboard
