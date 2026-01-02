#pragma once

#include <memory>
#include <optional>
#include <string>

#include "scheduler-offboard.pb.h"
#include "scheduler-sync-envelope.pb.h"

namespace ifex::offboard {

/// Create an offboard scheduler message from vehicle sync message
std::string encode_scheduler_offboard(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::sync_message_t& sync_msg,
    const std::string& fleet_id = "",
    const std::string& region = "",
    const std::string& bridge_id = "");

/// Decode an offboard scheduler message
std::optional<ifex::offboard::scheduler::scheduler_offboard_t>
decode_scheduler_offboard(const std::string& data);

/// Create a flattened job record from sync event
scheduler::job_record_t create_job_record(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::sync_event_t& event,
    const std::string& fleet_id = "",
    const std::string& region = "");

/// Create a flattened execution record from sync event
scheduler::execution_record_t create_execution_record(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::sync_event_t& event,
    const std::string& fleet_id = "",
    const std::string& region = "");

}  // namespace ifex::offboard
