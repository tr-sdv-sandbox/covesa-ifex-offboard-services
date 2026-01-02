#pragma once

#include <memory>
#include <optional>
#include <string>

#include "discovery-offboard.pb.h"
#include "discovery-sync-envelope.pb.h"

namespace ifex::offboard {

/// Create an offboard discovery message from vehicle sync message
/// @param vehicle_id Vehicle ID extracted from MQTT topic
/// @param sync_msg Original sync message from vehicle
/// @param fleet_id Fleet ID from enrichment (optional)
/// @param region Region from enrichment (optional)
/// @param bridge_id Bridge instance that received this message
/// @return Encoded offboard message
std::string encode_discovery_offboard(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::sync_message_t& sync_msg,
    const std::string& fleet_id = "",
    const std::string& region = "",
    const std::string& bridge_id = "");

/// Decode an offboard discovery message
std::optional<ifex::offboard::discovery::discovery_offboard_t>
decode_discovery_offboard(const std::string& data);

/// Create a flattened service record from sync event
/// Used for PostgreSQL insertion
ifex::offboard::discovery::service_record_t create_service_record(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::sync_event_t& event,
    const std::string& fleet_id = "",
    const std::string& region = "");

}  // namespace ifex::offboard
