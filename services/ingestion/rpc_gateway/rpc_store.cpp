#include "rpc_store.hpp"

#include <glog/logging.h>

#include "rpc_codec.hpp"

namespace ifex::offboard {

RpcStore::RpcStore(std::shared_ptr<PostgresClient> db)
    : db_(std::move(db)) {}

void RpcStore::upsert_vehicle(const std::string& vehicle_id,
                               const std::string& fleet_id,
                               const std::string& region) {
    // Use upsert_vehicle function
    db_->execute(
        "SELECT upsert_vehicle($1)",
        {vehicle_id});

    // Update fleet_id and region if provided
    if (!fleet_id.empty() || !region.empty()) {
        // TODO: Add fleet_id and region columns to vehicles table
        VLOG(1) << "Vehicle " << vehicle_id << " fleet=" << fleet_id
                << " region=" << region;
    }
}

void RpcStore::process_offboard_message(const rpc::rpc_offboard_t& msg) {
    // Extract metadata
    const std::string& vehicle_id = msg.metadata().vehicle_id();
    const std::string& fleet_id = msg.metadata().fleet_id();
    const std::string& region = msg.metadata().region();

    // Ensure vehicle exists with enrichment
    upsert_vehicle(vehicle_id, fleet_id, region);

    // Process based on direction
    if (msg.direction() == rpc::VEHICLE_TO_CLOUD && msg.has_response()) {
        record_response(msg.response());
    } else if (msg.direction() == rpc::CLOUD_TO_VEHICLE && msg.has_request()) {
        // This is unexpected - requests shouldn't come back to us
        LOG(WARNING) << "Received C2V request on response topic: "
                     << msg.request().correlation_id();
    } else {
        LOG(WARNING) << "Unknown RPC offboard message direction or content";
    }
}

void RpcStore::create_request(
    const std::string& vehicle_id,
    const swdv::dispatcher_rpc_envelope::rpc_request_t& request) {

    LOG(INFO) << "Creating RPC request: " << request.correlation_id()
              << " service=" << request.service_name()
              << " method=" << request.method_name();

    // Ensure vehicle exists
    upsert_vehicle(vehicle_id);

    db_->execute(
        R"(
        INSERT INTO rpc_requests (
            correlation_id, vehicle_id, service_name, method_name,
            parameters_json, timeout_ms, request_timestamp_ns
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        )",
        {
            request.correlation_id(),
            vehicle_id,
            request.service_name(),
            request.method_name(),
            request.parameters_json(),
            std::to_string(request.timeout_ms()),
            std::to_string(request.request_timestamp_ns())
        });
}

void RpcStore::record_response(
    const swdv::dispatcher_rpc_envelope::rpc_response_t& response) {

    LOG(INFO) << "Recording RPC response: " << response.correlation_id()
              << " status=" << rpc_status_name(response.status())
              << " duration=" << response.duration_ms() << "ms";

    db_->execute(
        R"(
        UPDATE rpc_requests SET
            response_status = $2,
            result_json = $3,
            error_message = $4,
            duration_ms = $5,
            responded_at = NOW()
        WHERE correlation_id = $1
        )",
        {
            response.correlation_id(),
            rpc_status_name(response.status()),
            response.result_json(),
            response.error_message(),
            std::to_string(response.duration_ms())
        });
}

std::optional<RpcRequestRecord> RpcStore::get_request(
    const std::string& correlation_id) {

    auto result = db_->execute(
        R"(
        SELECT correlation_id, vehicle_id, service_name, method_name,
               parameters_json, timeout_ms, response_status, result_json,
               error_message, responded_at IS NOT NULL as responded
        FROM rpc_requests
        WHERE correlation_id = $1
        )",
        {correlation_id});

    if (!result.ok() || result.num_rows() == 0) {
        return std::nullopt;
    }

    auto row = result.row(0);
    RpcRequestRecord record;
    record.correlation_id = row.get_string("correlation_id");
    record.vehicle_id = row.get_string("vehicle_id");
    record.service_name = row.get_string("service_name");
    record.method_name = row.get_string("method_name");
    record.parameters_json = row.get_string("parameters_json");
    record.timeout_ms = row.get_int("timeout_ms");
    record.response_status = row.get_string("response_status");
    record.result_json = row.get_string("result_json");
    record.error_message = row.get_string("error_message");
    record.responded = row.get_string("responded") == "t";

    return record;
}

std::vector<RpcRequestRecord> RpcStore::get_pending_requests(
    const std::string& vehicle_id) {

    auto result = db_->execute(
        R"(
        SELECT correlation_id, vehicle_id, service_name, method_name,
               parameters_json, timeout_ms, response_status, result_json,
               error_message, responded_at IS NOT NULL as responded
        FROM rpc_requests
        WHERE vehicle_id = $1 AND responded_at IS NULL
        ORDER BY created_at
        )",
        {vehicle_id});

    std::vector<RpcRequestRecord> records;

    if (!result.ok()) {
        return records;
    }

    for (int i = 0; i < result.num_rows(); ++i) {
        auto row = result.row(i);
        RpcRequestRecord record;
        record.correlation_id = row.get_string("correlation_id");
        record.vehicle_id = row.get_string("vehicle_id");
        record.service_name = row.get_string("service_name");
        record.method_name = row.get_string("method_name");
        record.parameters_json = row.get_string("parameters_json");
        record.timeout_ms = row.get_int("timeout_ms");
        record.response_status = row.get_string("response_status");
        record.result_json = row.get_string("result_json");
        record.error_message = row.get_string("error_message");
        record.responded = false;
        records.push_back(record);
    }

    return records;
}

int RpcStore::mark_timed_out_requests(int64_t timeout_threshold_ms) {
    auto result = db_->execute(
        R"(
        UPDATE rpc_requests SET
            response_status = 'TIMEOUT',
            error_message = 'Request timed out',
            responded_at = NOW()
        WHERE responded_at IS NULL
          AND EXTRACT(EPOCH FROM (NOW() - created_at)) * 1000 > timeout_ms
        )",
        {});

    return result.affected_rows();
}

}  // namespace ifex::offboard
