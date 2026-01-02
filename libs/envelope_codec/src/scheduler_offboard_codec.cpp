#include "scheduler_offboard_codec.hpp"

#include <chrono>
#include <glog/logging.h>

namespace ifex::offboard {

namespace {

uint64_t now_ns() {
    auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        now.time_since_epoch()).count();
}

std::string job_status_to_string(
    swdv::scheduler_sync_envelope::job_sync_status_t status) {
    switch (status) {
        case swdv::scheduler_sync_envelope::PENDING: return "PENDING";
        case swdv::scheduler_sync_envelope::RUNNING: return "RUNNING";
        case swdv::scheduler_sync_envelope::COMPLETED: return "COMPLETED";
        case swdv::scheduler_sync_envelope::FAILED: return "FAILED";
        case swdv::scheduler_sync_envelope::CANCELLED: return "CANCELLED";
        default: return "UNKNOWN";
    }
}

}  // namespace

std::string encode_scheduler_offboard(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::sync_message_t& sync_msg,
    const std::string& fleet_id,
    const std::string& region,
    const std::string& bridge_id) {

    scheduler::scheduler_offboard_t offboard;

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

std::optional<scheduler::scheduler_offboard_t>
decode_scheduler_offboard(const std::string& data) {
    scheduler::scheduler_offboard_t msg;
    if (msg.ParseFromString(data)) {
        return msg;
    }
    return std::nullopt;
}

scheduler::job_record_t create_job_record(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::sync_event_t& event,
    const std::string& fleet_id,
    const std::string& region) {

    scheduler::job_record_t record;

    record.set_vehicle_id(vehicle_id);
    record.set_job_id(event.job_id());
    record.set_sequence_number(event.sequence_number());

    if (event.has_job_info()) {
        const auto& job = event.job_info();
        record.set_title(job.title());
        record.set_service(job.service());
        record.set_method(job.method());
        record.set_parameters_json(job.parameters());
        record.set_scheduled_time(job.scheduled_time());
        record.set_recurrence_rule(job.recurrence_rule());
        record.set_next_run_time(job.next_run_time());
        record.set_status(job_status_to_string(job.status()));
        record.set_created_at_ms(job.created_at_ms());
        record.set_updated_at_ms(job.updated_at_ms());
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

scheduler::execution_record_t create_execution_record(
    const std::string& vehicle_id,
    const swdv::scheduler_sync_envelope::sync_event_t& event,
    const std::string& fleet_id,
    const std::string& region) {

    scheduler::execution_record_t record;

    record.set_vehicle_id(vehicle_id);
    record.set_job_id(event.job_id());
    record.set_sequence_number(event.sequence_number());

    if (event.has_execution_result()) {
        const auto& exec = event.execution_result();
        record.set_status(job_status_to_string(exec.status()));
        record.set_executed_at_ms(exec.executed_at_ms());
        record.set_duration_ms(exec.duration_ms());
        record.set_result_json(exec.result());
        record.set_error_message(exec.error_message());
        record.set_next_run_time(exec.next_run_time());
    }

    // Enrichment
    record.set_fleet_id(fleet_id);
    record.set_region(region);

    // Timestamps
    record.set_received_at_ns(now_ns());

    return record;
}

}  // namespace ifex::offboard
