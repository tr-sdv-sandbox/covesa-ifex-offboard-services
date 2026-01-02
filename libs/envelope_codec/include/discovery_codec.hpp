#pragma once

#include <optional>
#include <string>
#include <vector>

#include "discovery-sync-envelope.pb.h"

namespace ifex::offboard {

/// Decode discovery sync message from binary payload
std::optional<swdv::discovery_sync_envelope::sync_message_t>
decode_discovery_sync(const std::string& payload);

/// Encode discovery sync message to binary
std::string encode_discovery_sync(
    const swdv::discovery_sync_envelope::sync_message_t& msg);

/// Encode discovery sync ack
std::string encode_discovery_ack(
    uint64_t last_sequence,
    bool checksum_match,
    bool request_full_sync);

/// Helper to convert service_info_t to JSON-like string for logging
std::string service_info_to_string(
    const swdv::discovery_sync_envelope::service_info_t& info);

/// Helper to get event type name
const char* discovery_event_type_name(
    swdv::discovery_sync_envelope::sync_event_type_t type);

}  // namespace ifex::offboard
