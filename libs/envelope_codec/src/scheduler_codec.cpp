#include "scheduler_codec.hpp"

#include <sstream>

#include <glog/logging.h>

namespace ifex::offboard {

std::optional<swdv::scheduler_sync_envelope::sync_message_t>
decode_scheduler_sync(const std::string& payload) {
    swdv::scheduler_sync_envelope::sync_message_t msg;
    if (!msg.ParseFromString(payload)) {
        LOG(WARNING) << "Failed to parse scheduler sync message";
        return std::nullopt;
    }
    return msg;
}

std::string encode_scheduler_sync(
    const swdv::scheduler_sync_envelope::sync_message_t& msg) {
    std::string output;
    msg.SerializeToString(&output);
    return output;
}

std::string encode_scheduler_ack(
    uint64_t last_sequence,
    bool checksum_match,
    bool request_full_sync) {
    swdv::scheduler_sync_envelope::sync_ack_t ack;
    ack.set_last_sequence_received(last_sequence);
    ack.set_checksum_match(checksum_match);
    ack.set_request_full_sync(request_full_sync);

    std::string output;
    ack.SerializeToString(&output);
    return output;
}

std::string job_info_to_string(
    const swdv::scheduler_sync_envelope::job_info_t& info) {
    std::ostringstream ss;
    ss << "{"
       << "job_id=" << info.job_id()
       << ", title=" << info.title()
       << ", service=" << info.service()
       << ", method=" << info.method()
       << ", status=" << info.status()
       << ", scheduled_time=" << info.scheduled_time()
       << "}";
    return ss.str();
}

const char* scheduler_event_type_name(
    swdv::scheduler_sync_envelope::sync_event_type_t type) {
    switch (type) {
        case swdv::scheduler_sync_envelope::FULL_SYNC:
            return "FULL_SYNC";
        case swdv::scheduler_sync_envelope::JOB_CREATED:
            return "JOB_CREATED";
        case swdv::scheduler_sync_envelope::JOB_UPDATED:
            return "JOB_UPDATED";
        case swdv::scheduler_sync_envelope::JOB_DELETED:
            return "JOB_DELETED";
        case swdv::scheduler_sync_envelope::JOB_EXECUTED:
            return "JOB_EXECUTED";
        case swdv::scheduler_sync_envelope::HEARTBEAT:
            return "HEARTBEAT";
        default:
            return "UNKNOWN";
    }
}

std::optional<swdv::scheduler_command_envelope::scheduler_command_ack_t>
decode_scheduler_command_ack(const std::string& payload) {
    swdv::scheduler_command_envelope::scheduler_command_ack_t ack;
    if (!ack.ParseFromString(payload)) {
        return std::nullopt;
    }
    return ack;
}

}  // namespace ifex::offboard
