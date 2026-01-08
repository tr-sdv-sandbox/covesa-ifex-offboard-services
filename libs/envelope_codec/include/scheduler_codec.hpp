#pragma once

#include <optional>
#include <string>

#include "scheduler-sync-envelope.pb.h"
#include "scheduler-command-envelope.pb.h"

namespace ifex::offboard {

/// Decode scheduler sync message from binary payload
std::optional<swdv::scheduler_sync_envelope::sync_message_t>
decode_scheduler_sync(const std::string& payload);

/// Encode scheduler sync message to binary
std::string encode_scheduler_sync(
    const swdv::scheduler_sync_envelope::sync_message_t& msg);

/// Encode scheduler sync ack
std::string encode_scheduler_ack(
    uint64_t last_sequence,
    bool checksum_match,
    bool request_full_sync);

/// Helper to convert job_info_t to JSON-like string for logging
std::string job_info_to_string(
    const swdv::scheduler_sync_envelope::job_info_t& info);

/// Helper to get event type name
const char* scheduler_event_type_name(
    swdv::scheduler_sync_envelope::sync_event_type_t type);

/// Decode scheduler command ack from binary payload
std::optional<swdv::scheduler_command_envelope::scheduler_command_ack_t>
decode_scheduler_command_ack(const std::string& payload);

}  // namespace ifex::offboard
