#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "discovery-sync-envelope.pb.h"
#include "postgres_client.hpp"

namespace ifex::offboard {

/// Callback for sending schema requests to vehicle (via MQTT c2v)
using SchemaRequestCallback = std::function<void(
    const std::string& vehicle_id,
    const std::vector<std::string>& unknown_hashes)>;

/// PostgreSQL store for discovery sync data (hash-based protocol)
class DiscoveryStore {
public:
    explicit DiscoveryStore(std::shared_ptr<PostgresClient> db);

    /// Set callback for requesting unknown schemas from vehicle
    void set_schema_request_callback(SchemaRequestCallback callback);

    // =========================================================================
    // Hash-based protocol
    // =========================================================================

    /// Process hash manifest from vehicle (just hashes, no full schema)
    /// Returns list of unknown hashes that need to be requested
    std::vector<std::string> process_hash_manifest(
        const std::string& vehicle_id,
        const swdv::discovery_sync_envelope::hash_list_t& manifest);

    /// Process schema response from vehicle (hash → YAML mapping)
    void process_schema_response(
        const std::string& vehicle_id,
        const swdv::discovery_sync_envelope::schema_map_t& schemas);

    /// Check which hashes are unknown (not in schema_registry)
    std::vector<std::string> get_unknown_hashes(
        const std::vector<std::string>& hashes);

    /// Store a schema by hash
    void store_schema(
        const std::string& schema_hash,
        const std::string& ifex_schema,
        const std::string& vehicle_id);

    /// Link vehicle to schema hash
    void link_vehicle_schema(
        const std::string& vehicle_id,
        const std::string& schema_hash);

    /// Ensure vehicle exists in database
    void upsert_vehicle(const std::string& vehicle_id,
                        const std::string& fleet_id = "",
                        const std::string& region = "");

private:
    std::shared_ptr<PostgresClient> db_;
    SchemaRequestCallback schema_request_callback_;

    /// Parse IFEX YAML and extract methods, structs, enums as JSONB
    static void parse_ifex_schema(
        const std::string& ifex_yaml,
        std::string& service_name,
        std::string& version,
        std::string& methods_json,
        std::string& structs_json,
        std::string& enums_json);
};

}  // namespace ifex::offboard
