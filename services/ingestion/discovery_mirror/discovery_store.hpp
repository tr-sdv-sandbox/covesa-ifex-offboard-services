#pragma once

#include <memory>
#include <string>

#include "discovery-offboard.pb.h"
#include "discovery-sync-envelope.pb.h"
#include "postgres_client.hpp"

namespace ifex::offboard {

/// PostgreSQL store for discovery sync data
class DiscoveryStore {
public:
    explicit DiscoveryStore(std::shared_ptr<PostgresClient> db);

    /// Ensure vehicle exists in database
    void upsert_vehicle(const std::string& vehicle_id,
                        const std::string& fleet_id = "",
                        const std::string& region = "");

    /// Process an offboard discovery message (from Kafka)
    void process_offboard_message(
        const discovery::discovery_offboard_t& msg);

    /// Handle FULL_SYNC event - replace all services for vehicle
    void handle_full_sync(
        const std::string& vehicle_id,
        const swdv::discovery_sync_envelope::sync_message_t& msg);

    /// Handle SERVICE_REGISTERED event
    void handle_service_registered(
        const std::string& vehicle_id,
        const swdv::discovery_sync_envelope::service_info_t& info);

    /// Handle SERVICE_UNREGISTERED event
    void handle_service_unregistered(
        const std::string& vehicle_id,
        const std::string& registration_id);

    /// Handle SERVICE_STATUS_CHANGED event
    void handle_service_status_changed(
        const std::string& vehicle_id,
        const swdv::discovery_sync_envelope::service_info_t& info);

    /// Handle HEARTBEAT event
    void handle_heartbeat(
        const std::string& vehicle_id,
        const std::string& registration_id,
        uint64_t timestamp_ns);

    /// Update sync state
    void update_sync_state(
        const std::string& vehicle_id,
        uint64_t sequence,
        uint32_t checksum);

    /// Get last processed sequence for vehicle
    uint64_t get_last_sequence(const std::string& vehicle_id);

private:
    std::shared_ptr<PostgresClient> db_;

    /// Convert transport_type enum to string
    static std::string transport_type_to_string(
        swdv::discovery_sync_envelope::transport_type_t type);

    /// Convert namespaces to JSON
    static std::string namespaces_to_json(
        const google::protobuf::RepeatedPtrField<
            swdv::discovery_sync_envelope::namespace_info_t>& namespaces);
};

}  // namespace ifex::offboard
