#include "discovery_store.hpp"

#include <sstream>

#include <glog/logging.h>

#include "discovery_codec.hpp"

namespace ifex::offboard {

DiscoveryStore::DiscoveryStore(std::shared_ptr<PostgresClient> db)
    : db_(std::move(db)) {}

void DiscoveryStore::upsert_vehicle(const std::string& vehicle_id,
                                     const std::string& fleet_id,
                                     const std::string& region) {
    // Use upsert_vehicle function, add fleet/region if we have them
    db_->execute(
        "SELECT upsert_vehicle($1)",
        {vehicle_id});

    // Update fleet_id and region if provided
    if (!fleet_id.empty() || !region.empty()) {
        // TODO: Add fleet_id and region columns to vehicles table
        // For now, just log the enrichment data
        VLOG(1) << "Vehicle " << vehicle_id << " fleet=" << fleet_id
                << " region=" << region;
    }
}

void DiscoveryStore::process_offboard_message(
    const discovery::discovery_offboard_t& msg) {

    // Extract vehicle_id from offboard metadata (verified by ACL)
    const std::string& vehicle_id = msg.metadata().vehicle_id();
    const std::string& fleet_id = msg.metadata().fleet_id();
    const std::string& region = msg.metadata().region();

    // Get the embedded sync message
    const auto& sync_msg = msg.sync_message();

    // Ensure vehicle exists with enrichment
    upsert_vehicle(vehicle_id, fleet_id, region);

    // Process each event
    for (const auto& event : sync_msg.events()) {
        LOG(INFO) << "Processing " << discovery_event_type_name(event.event_type())
                  << " for vehicle " << vehicle_id;

        switch (event.event_type()) {
            case swdv::discovery_sync_envelope::FULL_SYNC:
                handle_full_sync(vehicle_id, sync_msg);
                break;

            case swdv::discovery_sync_envelope::SERVICE_REGISTERED:
                if (event.has_service_info()) {
                    handle_service_registered(vehicle_id, event.service_info());
                }
                break;

            case swdv::discovery_sync_envelope::SERVICE_UNREGISTERED:
                handle_service_unregistered(vehicle_id, event.registration_id());
                break;

            case swdv::discovery_sync_envelope::SERVICE_STATUS_CHANGED:
                if (event.has_service_info()) {
                    handle_service_status_changed(vehicle_id, event.service_info());
                }
                break;

            case swdv::discovery_sync_envelope::HEARTBEAT:
                handle_heartbeat(vehicle_id, event.registration_id(),
                                event.timestamp_ns());
                break;
        }
    }

    // Update sync state
    if (!sync_msg.events().empty()) {
        uint64_t last_seq = sync_msg.events().rbegin()->sequence_number();
        update_sync_state(vehicle_id, last_seq, sync_msg.state_checksum());
    }
}

void DiscoveryStore::handle_full_sync(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::sync_message_t& msg) {

    LOG(INFO) << "Full sync for vehicle " << vehicle_id
              << " with " << msg.total_services() << " services";

    // Start transaction
    db_->begin_transaction();

    // Delete all existing services for this vehicle
    db_->execute(
        "DELETE FROM services WHERE vehicle_id = $1",
        {vehicle_id});

    // Insert all services from the sync
    for (const auto& event : msg.events()) {
        if (event.event_type() == swdv::discovery_sync_envelope::FULL_SYNC &&
            event.has_service_info()) {
            handle_service_registered(vehicle_id, event.service_info());
        }
    }

    db_->commit();
}

void DiscoveryStore::handle_service_registered(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::service_info_t& info) {

    LOG(INFO) << "Registering service: " << service_info_to_string(info);

    db_->execute(
        R"(
        INSERT INTO services (
            vehicle_id, registration_id, service_name, version, description,
            endpoint_address, transport_type, status, last_heartbeat_ms, namespaces
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb)
        ON CONFLICT (vehicle_id, registration_id)
        DO UPDATE SET
            service_name = EXCLUDED.service_name,
            version = EXCLUDED.version,
            description = EXCLUDED.description,
            endpoint_address = EXCLUDED.endpoint_address,
            transport_type = EXCLUDED.transport_type,
            status = EXCLUDED.status,
            last_heartbeat_ms = EXCLUDED.last_heartbeat_ms,
            namespaces = EXCLUDED.namespaces,
            updated_at = NOW()
        )",
        {
            vehicle_id,
            info.registration_id(),
            info.name(),
            info.version(),
            info.description(),
            info.endpoint().address(),
            transport_type_to_string(info.endpoint().transport()),
            std::to_string(info.status()),
            std::to_string(info.last_heartbeat_ms()),
            namespaces_to_json(info.namespaces())
        });
}

void DiscoveryStore::handle_service_unregistered(
    const std::string& vehicle_id,
    const std::string& registration_id) {

    LOG(INFO) << "Unregistering service: " << registration_id
              << " from vehicle " << vehicle_id;

    db_->execute(
        "DELETE FROM services WHERE vehicle_id = $1 AND registration_id = $2",
        {vehicle_id, registration_id});
}

void DiscoveryStore::handle_service_status_changed(
    const std::string& vehicle_id,
    const swdv::discovery_sync_envelope::service_info_t& info) {

    LOG(INFO) << "Status changed for service " << info.registration_id()
              << ": status=" << info.status();

    db_->execute(
        R"(
        UPDATE services
        SET status = $3,
            last_heartbeat_ms = $4,
            updated_at = NOW()
        WHERE vehicle_id = $1 AND registration_id = $2
        )",
        {
            vehicle_id,
            info.registration_id(),
            std::to_string(info.status()),
            std::to_string(info.last_heartbeat_ms())
        });
}

void DiscoveryStore::handle_heartbeat(
    const std::string& vehicle_id,
    const std::string& registration_id,
    uint64_t timestamp_ns) {

    VLOG(1) << "Heartbeat from " << registration_id
            << " on vehicle " << vehicle_id;

    uint64_t timestamp_ms = timestamp_ns / 1000000;

    db_->execute(
        R"(
        UPDATE services
        SET last_heartbeat_ms = $3, updated_at = NOW()
        WHERE vehicle_id = $1 AND registration_id = $2
        )",
        {vehicle_id, registration_id, std::to_string(timestamp_ms)});

    // Also update vehicle last_seen_at
    db_->execute(
        "UPDATE vehicles SET last_seen_at = NOW() WHERE vehicle_id = $1",
        {vehicle_id});
}

void DiscoveryStore::update_sync_state(
    const std::string& vehicle_id,
    uint64_t sequence,
    uint32_t checksum) {

    db_->execute(
        "SELECT update_sync_state($1, 'discovery', $2, $3)",
        {vehicle_id, std::to_string(sequence), std::to_string(checksum)});
}

uint64_t DiscoveryStore::get_last_sequence(const std::string& vehicle_id) {
    auto result = db_->execute_scalar(
        "SELECT discovery_sequence FROM sync_state WHERE vehicle_id = $1",
        {vehicle_id});

    if (result) {
        return std::stoull(*result);
    }
    return 0;
}

std::string DiscoveryStore::transport_type_to_string(
    swdv::discovery_sync_envelope::transport_type_t type) {
    switch (type) {
        case swdv::discovery_sync_envelope::GRPC:
            return "grpc";
        case swdv::discovery_sync_envelope::HTTP_REST:
            return "http_rest";
        case swdv::discovery_sync_envelope::DBUS:
            return "dbus";
        case swdv::discovery_sync_envelope::SOMEIP:
            return "someip";
        case swdv::discovery_sync_envelope::MQTT:
            return "mqtt";
        default:
            return "unknown";
    }
}

std::string DiscoveryStore::namespaces_to_json(
    const google::protobuf::RepeatedPtrField<
        swdv::discovery_sync_envelope::namespace_info_t>& namespaces) {

    std::ostringstream json;
    json << "[";

    bool first = true;
    for (const auto& ns : namespaces) {
        if (!first) json << ",";
        first = false;

        json << "{\"name\":\"" << ns.name() << "\",\"methods\":[";

        bool first_method = true;
        for (const auto& method : ns.methods()) {
            if (!first_method) json << ",";
            first_method = false;
            json << "{\"name\":\"" << method.name() << "\""
                 << ",\"description\":\"" << method.description() << "\"}";
        }

        json << "]}";
    }

    json << "]";
    return json.str();
}

}  // namespace ifex::offboard
