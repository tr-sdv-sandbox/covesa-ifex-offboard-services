#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "postgres_client.hpp"

namespace ifex {
namespace cloud {
namespace discovery {

// Data structs for query results (distinct from protobuf-generated classes)
namespace query {

/**
 * Vehicle summary for listing
 */
struct VehicleSummaryData {
    std::string vehicle_id;
    std::string fleet_id;
    std::string region;
    std::string model;
    int year = 0;
    std::string owner;
    int service_count = 0;
    int job_count = 0;
    int64_t last_seen_ms = 0;
    bool is_online = false;
    int64_t created_at_ms = 0;
    int64_t updated_at_ms = 0;
};

/**
 * Service info from schema_registry (hash-based protocol)
 */
struct ServiceInfoData {
    std::string schema_hash;
    std::string service_name;
    std::string version;
    std::string ifex_schema;       // Full IFEX YAML
    std::string methods_json;      // Pre-parsed methods
    std::string structs_json;      // Pre-parsed struct definitions
    std::string enums_json;        // Pre-parsed enum definitions
    int64_t first_seen_ms = 0;
    int64_t last_seen_ms = 0;
};

/**
 * Service location (service + vehicle info)
 */
struct ServiceLocationData {
    std::string vehicle_id;
    std::string fleet_id;
    std::string region;
    ServiceInfoData service;
};

/**
 * Service statistics
 */
struct ServiceStatsData {
    std::string service_name;
    int vehicle_count = 0;
    int available_count = 0;
    std::map<std::string, int> by_version;
    std::map<std::string, int> by_region;
};

}  // namespace query

/**
 * Query result with pagination
 */
template<typename T>
struct QueryResult {
    std::vector<T> items;
    int total_count = 0;
    std::string next_page_token;
};

/**
 * PostgreSQL query helper for fleet-wide discovery queries
 */
class DiscoveryQuery {
public:
    explicit DiscoveryQuery(std::shared_ptr<ifex::offboard::PostgresClient> db);

    /**
     * List vehicles with optional filters
     */
    QueryResult<query::VehicleSummaryData> list_vehicles(
        const std::string& vehicle_id_pattern,
        const std::string& fleet_id_filter,
        const std::string& region_filter,
        bool online_only,
        bool with_services_only,
        int page_size,
        int offset);

    /**
     * Get services for a specific vehicle (from schema_registry via vehicle_schemas)
     */
    std::vector<query::ServiceInfoData> get_vehicle_services(
        const std::string& vehicle_id);

    /**
     * Query services by name pattern
     */
    QueryResult<query::ServiceLocationData> query_services_by_name(
        const std::string& name_pattern,
        const std::string& fleet_id_filter,
        const std::string& region_filter,
        bool available_only,
        int page_size,
        int offset);

    /**
     * Get fleet-wide service statistics
     */
    std::vector<query::ServiceStatsData> get_fleet_service_stats(
        const std::string& fleet_id_filter,
        const std::string& region_filter);

    /**
     * Get fleet summary (total vehicles, online vehicles, total services)
     */
    struct FleetSummary {
        int total_vehicles = 0;
        int online_vehicles = 0;
        int total_services = 0;
    };
    FleetSummary get_fleet_summary(
        const std::string& fleet_id_filter,
        const std::string& region_filter);

    /**
     * Find vehicles that have a specific service
     */
    QueryResult<query::VehicleSummaryData> find_vehicles_with_service(
        const std::string& service_name,
        const std::string& method_name,
        const std::string& fleet_id_filter,
        const std::string& region_filter,
        bool available_only,
        int page_size,
        int offset);

    /**
     * Get sync state for a vehicle
     */
    struct SyncState {
        int64_t discovery_sequence = 0;
        uint32_t state_checksum = 0;
        int64_t updated_at_ms = 0;
    };
    std::optional<SyncState> get_sync_state(const std::string& vehicle_id);

private:
    std::shared_ptr<ifex::offboard::PostgresClient> db_;

    // Helper to convert SQL LIKE pattern
    std::string to_sql_pattern(const std::string& pattern);

    // Parse page token (base64 encoded offset)
    int parse_page_token(const std::string& token);
    std::string make_page_token(int offset);
};

}  // namespace discovery
}  // namespace cloud
}  // namespace ifex
