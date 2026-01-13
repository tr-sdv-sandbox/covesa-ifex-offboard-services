#include "discovery_query.hpp"

#include <algorithm>
#include <sstream>

#include <glog/logging.h>

namespace ifex {
namespace cloud {
namespace discovery {

namespace {

/// Convert status string to integer for ServiceStatus enum
/// Values: 0=UNKNOWN, 1=AVAILABLE, 2=UNAVAILABLE, 3=DEGRADED
int status_string_to_int(const std::string& status) {
    if (status == "available") return 1;
    if (status == "unavailable") return 2;
    if (status == "degraded") return 3;
    return 0;  // unknown
}

}  // namespace

DiscoveryQuery::DiscoveryQuery(std::shared_ptr<ifex::offboard::PostgresClient> db)
    : db_(std::move(db)) {
}

std::string DiscoveryQuery::to_sql_pattern(const std::string& pattern) {
    // Convert wildcard pattern (* -> %) to SQL LIKE pattern
    std::string sql_pattern = pattern;
    std::replace(sql_pattern.begin(), sql_pattern.end(), '*', '%');
    return sql_pattern;
}

int DiscoveryQuery::parse_page_token(const std::string& token) {
    if (token.empty()) return 0;
    try {
        return std::stoi(token);
    } catch (...) {
        return 0;
    }
}

std::string DiscoveryQuery::make_page_token(int offset) {
    return std::to_string(offset);
}

QueryResult<query::VehicleSummaryData> DiscoveryQuery::list_vehicles(
    const std::string& vehicle_id_pattern,
    const std::string& fleet_id_filter,
    const std::string& region_filter,
    bool online_only,
    bool with_services_only,
    int page_size,
    int offset) {

    QueryResult<query::VehicleSummaryData> result;

    // Build WHERE clause
    std::vector<std::string> conditions;
    std::vector<std::string> params;
    int param_idx = 1;

    if (!vehicle_id_pattern.empty()) {
        conditions.push_back("e.vehicle_id ILIKE $" + std::to_string(param_idx++));
        params.push_back(to_sql_pattern(vehicle_id_pattern));
    }
    if (!fleet_id_filter.empty()) {
        conditions.push_back("e.fleet_id = $" + std::to_string(param_idx++));
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        conditions.push_back("e.region = $" + std::to_string(param_idx++));
        params.push_back(region_filter);
    }
    if (with_services_only) {
        conditions.push_back("COALESCE(s.service_count, 0) > 0");
    }
    if (online_only) {
        conditions.push_back("v.is_online = true");
    }

    std::string where_clause = conditions.empty() ? "" :
        "WHERE " + [&]() {
            std::string s;
            for (size_t i = 0; i < conditions.size(); i++) {
                if (i > 0) s += " AND ";
                s += conditions[i];
            }
            return s;
        }();

    // Count query
    std::string count_query = R"(
        SELECT COUNT(DISTINCT e.vehicle_id)
        FROM vehicle_enrichment e
        LEFT JOIN vehicles v ON e.vehicle_id = v.vehicle_id
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as service_count
            FROM vehicle_schemas
            GROUP BY vehicle_id
        ) s ON e.vehicle_id = s.vehicle_id
    )" + where_clause;

    auto count_result = db_->execute(count_query, params);
    if (count_result.num_rows() > 0) {
        result.total_count = count_result.row(0).get_int("count");
    }

    // Data query
    std::string query = R"(
        SELECT
            e.vehicle_id,
            e.fleet_id,
            e.region,
            e.model,
            e.year,
            e.owner,
            COALESCE(s.service_count, 0) as service_count,
            COALESCE(j.job_count, 0) as job_count,
            v.last_seen_at,
            v.is_online,
            (EXTRACT(EPOCH FROM e.created_at) * 1000)::bigint AS created_at_ms,
            (EXTRACT(EPOCH FROM e.updated_at) * 1000)::bigint AS updated_at_ms
        FROM vehicle_enrichment e
        LEFT JOIN vehicles v ON e.vehicle_id = v.vehicle_id
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as service_count
            FROM vehicle_schemas
            GROUP BY vehicle_id
        ) s ON e.vehicle_id = s.vehicle_id
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as job_count
            FROM jobs
            GROUP BY vehicle_id
        ) j ON e.vehicle_id = j.vehicle_id
    )" + where_clause + R"(
        ORDER BY e.vehicle_id
    )";

    // Add LIMIT and OFFSET with explicit parameter order to avoid undefined evaluation order
    int limit_idx = param_idx++;
    int offset_idx = param_idx++;
    query += " LIMIT $" + std::to_string(limit_idx);
    query += " OFFSET $" + std::to_string(offset_idx);

    params.push_back(std::to_string(page_size));
    params.push_back(std::to_string(offset));

    auto data_result = db_->execute(query, params);
    for (int i = 0; i < data_result.num_rows(); i++) {
        auto row = data_result.row(i);
        query::VehicleSummaryData vs;
        vs.vehicle_id = row.get_string("vehicle_id");
        vs.fleet_id = row.get_string("fleet_id");
        vs.region = row.get_string("region");
        vs.model = row.get_string("model");
        vs.year = row.is_null("year") ? 0 : row.get_int("year");
        vs.owner = row.is_null("owner") ? "" : row.get_string("owner");
        vs.service_count = row.is_null("service_count") ? 0 : row.get_int("service_count");
        vs.job_count = row.is_null("job_count") ? 0 : row.get_int("job_count");
        vs.is_online = row.get_string("is_online") == "t";
        vs.created_at_ms = row.is_null("created_at_ms") ? 0 : row.get_int64("created_at_ms");
        vs.updated_at_ms = row.is_null("updated_at_ms") ? 0 : row.get_int64("updated_at_ms");
        result.items.push_back(std::move(vs));
    }

    // Set next page token if there are more results
    int next_offset = offset + result.items.size();
    if (next_offset < result.total_count) {
        result.next_page_token = make_page_token(next_offset);
    }

    return result;
}

std::vector<query::ServiceInfoData> DiscoveryQuery::get_vehicle_services(
    const std::string& vehicle_id) {

    std::vector<query::ServiceInfoData> services;

    // Query schema_registry via vehicle_schemas junction table
    std::string query = R"(
        SELECT
            sr.schema_hash,
            sr.service_name,
            sr.version,
            sr.ifex_schema,
            sr.methods::text AS methods_json,
            sr.struct_definitions::text AS structs_json,
            sr.enum_definitions::text AS enums_json,
            (EXTRACT(EPOCH FROM sr.first_seen_at) * 1000000000)::bigint AS first_seen_ms,
            (EXTRACT(EPOCH FROM vs.last_seen_at) * 1000000000)::bigint AS last_seen_ms
        FROM vehicle_schemas vs
        JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash
        WHERE vs.vehicle_id = $1
        ORDER BY sr.service_name
    )";

    auto result = db_->execute(query, {vehicle_id});
    for (int i = 0; i < result.num_rows(); i++) {
        auto row = result.row(i);
        query::ServiceInfoData si;
        si.schema_hash = row.get_string("schema_hash");
        si.service_name = row.get_string("service_name");
        si.version = row.get_string("version");
        si.ifex_schema = row.get_string("ifex_schema");
        si.methods_json = row.get_string("methods_json");
        si.structs_json = row.get_string("structs_json");
        si.enums_json = row.get_string("enums_json");
        si.first_seen_ms = row.is_null("first_seen_ms") ? 0 : row.get_int64("first_seen_ms");
        si.last_seen_ms = row.is_null("last_seen_ms") ? 0 : row.get_int64("last_seen_ms");
        services.push_back(std::move(si));
    }

    return services;
}

QueryResult<query::ServiceLocationData> DiscoveryQuery::query_services_by_name(
    const std::string& name_pattern,
    const std::string& fleet_id_filter,
    const std::string& region_filter,
    bool available_only,
    int page_size,
    int offset) {

    QueryResult<query::ServiceLocationData> result;

    std::vector<std::string> conditions;
    std::vector<std::string> params;
    int param_idx = 1;

    // Name pattern (required)
    conditions.push_back("sr.service_name LIKE $" + std::to_string(param_idx++));
    params.push_back(to_sql_pattern(name_pattern));

    if (!fleet_id_filter.empty()) {
        conditions.push_back("e.fleet_id = $" + std::to_string(param_idx++));
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        conditions.push_back("e.region = $" + std::to_string(param_idx++));
        params.push_back(region_filter);
    }
    // Note: available_only not applicable for hash-based protocol (no runtime status)

    std::string where_clause = "WHERE " + [&]() {
        std::string s;
        for (size_t i = 0; i < conditions.size(); i++) {
            if (i > 0) s += " AND ";
            s += conditions[i];
        }
        return s;
    }();

    // Count query
    std::string count_query = R"(
        SELECT COUNT(*)
        FROM vehicle_schemas vs
        JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash
        LEFT JOIN vehicle_enrichment e ON vs.vehicle_id = e.vehicle_id
    )" + where_clause;

    auto count_result = db_->execute(count_query, params);
    if (count_result.num_rows() > 0) {
        result.total_count = count_result.row(0).get_int("count");
    }

    // Data query
    std::string query = R"(
        SELECT
            vs.vehicle_id,
            COALESCE(e.fleet_id, '') AS fleet_id,
            COALESCE(e.region, '') AS region,
            sr.schema_hash,
            sr.service_name,
            sr.version,
            sr.ifex_schema,
            sr.methods::text AS methods_json,
            sr.struct_definitions::text AS structs_json,
            sr.enum_definitions::text AS enums_json,
            (EXTRACT(EPOCH FROM sr.first_seen_at) * 1000000000)::bigint AS first_seen_ms,
            (EXTRACT(EPOCH FROM vs.last_seen_at) * 1000000000)::bigint AS last_seen_ms
        FROM vehicle_schemas vs
        JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash
        LEFT JOIN vehicle_enrichment e ON vs.vehicle_id = e.vehicle_id
    )" + where_clause + R"(
        ORDER BY sr.service_name, vs.vehicle_id
    )";

    // Add LIMIT and OFFSET with explicit parameter order
    int limit_idx = param_idx++;
    int offset_idx = param_idx++;
    query += " LIMIT $" + std::to_string(limit_idx);
    query += " OFFSET $" + std::to_string(offset_idx);

    params.push_back(std::to_string(page_size));
    params.push_back(std::to_string(offset));

    auto data_result = db_->execute(query, params);
    for (int i = 0; i < data_result.num_rows(); i++) {
        auto row = data_result.row(i);
        query::ServiceLocationData sl;
        sl.vehicle_id = row.get_string("vehicle_id");
        sl.fleet_id = row.get_string("fleet_id");
        sl.region = row.get_string("region");
        sl.service.schema_hash = row.get_string("schema_hash");
        sl.service.service_name = row.get_string("service_name");
        sl.service.version = row.get_string("version");
        sl.service.ifex_schema = row.get_string("ifex_schema");
        sl.service.methods_json = row.get_string("methods_json");
        sl.service.structs_json = row.get_string("structs_json");
        sl.service.enums_json = row.get_string("enums_json");
        sl.service.first_seen_ms = row.is_null("first_seen_ms") ? 0 : row.get_int64("first_seen_ms");
        sl.service.last_seen_ms = row.is_null("last_seen_ms") ? 0 : row.get_int64("last_seen_ms");
        result.items.push_back(std::move(sl));
    }

    int next_offset = offset + result.items.size();
    if (next_offset < result.total_count) {
        result.next_page_token = make_page_token(next_offset);
    }

    return result;
}

std::vector<query::ServiceStatsData> DiscoveryQuery::get_fleet_service_stats(
    const std::string& fleet_id_filter,
    const std::string& region_filter) {

    std::vector<query::ServiceStatsData> stats;

    std::vector<std::string> conditions;
    std::vector<std::string> params;
    int param_idx = 1;

    if (!fleet_id_filter.empty()) {
        conditions.push_back("e.fleet_id = $" + std::to_string(param_idx++));
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        conditions.push_back("e.region = $" + std::to_string(param_idx++));
        params.push_back(region_filter);
    }

    std::string where_clause = conditions.empty() ? "" :
        "WHERE " + [&]() {
            std::string s;
            for (size_t i = 0; i < conditions.size(); i++) {
                if (i > 0) s += " AND ";
                s += conditions[i];
            }
            return s;
        }();

    std::string query = R"(
        SELECT
            sr.service_name,
            COUNT(DISTINCT vs.vehicle_id) as vehicle_count,
            COUNT(DISTINCT vs.vehicle_id) as available_count
        FROM vehicle_schemas vs
        JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash
        JOIN vehicle_enrichment e ON vs.vehicle_id = e.vehicle_id
    )" + where_clause + R"(
        GROUP BY sr.service_name
        ORDER BY vehicle_count DESC
    )";

    auto result = db_->execute(query, params);
    for (int i = 0; i < result.num_rows(); i++) {
        auto row = result.row(i);
        query::ServiceStatsData ss;
        ss.service_name = row.get_string("service_name");
        ss.vehicle_count = row.get_int("vehicle_count");
        ss.available_count = row.get_int("available_count");
        stats.push_back(std::move(ss));
    }

    return stats;
}

DiscoveryQuery::FleetSummary DiscoveryQuery::get_fleet_summary(
    const std::string& fleet_id_filter,
    const std::string& region_filter) {

    FleetSummary summary;

    std::vector<std::string> conditions;
    std::vector<std::string> params;
    int param_idx = 1;

    if (!fleet_id_filter.empty()) {
        conditions.push_back("e.fleet_id = $" + std::to_string(param_idx++));
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        conditions.push_back("e.region = $" + std::to_string(param_idx++));
        params.push_back(region_filter);
    }

    std::string where_clause = conditions.empty() ? "" :
        "WHERE " + [&]() {
            std::string s;
            for (size_t i = 0; i < conditions.size(); i++) {
                if (i > 0) s += " AND ";
                s += conditions[i];
            }
            return s;
        }();

    // Total vehicles
    std::string count_query = "SELECT COUNT(*) FROM vehicle_enrichment e " + where_clause;
    auto result = db_->execute(count_query, params);
    if (result.num_rows() > 0) {
        summary.total_vehicles = result.row(0).get_int("count");
    }

    // Online vehicles (with services)
    std::string online_query = R"(
        SELECT COUNT(DISTINCT vs.vehicle_id)
        FROM vehicle_schemas vs
        JOIN vehicle_enrichment e ON vs.vehicle_id = e.vehicle_id
    )" + where_clause;
    result = db_->execute(online_query, params);
    if (result.num_rows() > 0) {
        summary.online_vehicles = result.row(0).get_int("count");
    }

    // Total services
    std::string services_query = R"(
        SELECT COUNT(*)
        FROM vehicle_schemas vs
        JOIN vehicle_enrichment e ON vs.vehicle_id = e.vehicle_id
    )" + where_clause;
    result = db_->execute(services_query, params);
    if (result.num_rows() > 0) {
        summary.total_services = result.row(0).get_int("count");
    }

    return summary;
}

QueryResult<query::VehicleSummaryData> DiscoveryQuery::find_vehicles_with_service(
    const std::string& service_name,
    const std::string& method_name,
    const std::string& fleet_id_filter,
    const std::string& region_filter,
    bool available_only,
    int page_size,
    int offset) {

    QueryResult<query::VehicleSummaryData> result;

    std::vector<std::string> conditions;
    std::vector<std::string> params;
    int param_idx = 1;

    conditions.push_back("sr.service_name = $" + std::to_string(param_idx++));
    params.push_back(service_name);

    if (!fleet_id_filter.empty()) {
        conditions.push_back("e.fleet_id = $" + std::to_string(param_idx++));
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        conditions.push_back("e.region = $" + std::to_string(param_idx++));
        params.push_back(region_filter);
    }
    // Note: available_only not applicable for hash-based protocol (no runtime status)

    std::string where_clause = "WHERE " + [&]() {
        std::string s;
        for (size_t i = 0; i < conditions.size(); i++) {
            if (i > 0) s += " AND ";
            s += conditions[i];
        }
        return s;
    }();

    // Count
    std::string count_query = R"(
        SELECT COUNT(DISTINCT vs.vehicle_id)
        FROM vehicle_schemas vs
        JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash
        JOIN vehicle_enrichment e ON vs.vehicle_id = e.vehicle_id
    )" + where_clause;

    auto count_result = db_->execute(count_query, params);
    if (count_result.num_rows() > 0) {
        result.total_count = count_result.row(0).get_int("count");
    }

    // Data
    std::string query = R"(
        SELECT DISTINCT
            e.vehicle_id,
            e.fleet_id,
            e.region,
            e.model,
            e.year,
            e.owner,
            v.is_online,
            (EXTRACT(EPOCH FROM e.created_at) * 1000)::bigint AS created_at_ms,
            (EXTRACT(EPOCH FROM e.updated_at) * 1000)::bigint AS updated_at_ms
        FROM vehicle_enrichment e
        JOIN vehicle_schemas vs ON e.vehicle_id = vs.vehicle_id
        JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash
        LEFT JOIN vehicles v ON e.vehicle_id = v.vehicle_id
    )" + where_clause + R"(
        ORDER BY e.vehicle_id
    )";

    // Add LIMIT and OFFSET with explicit parameter order
    int limit_idx = param_idx++;
    int offset_idx = param_idx++;
    query += " LIMIT $" + std::to_string(limit_idx);
    query += " OFFSET $" + std::to_string(offset_idx);

    params.push_back(std::to_string(page_size));
    params.push_back(std::to_string(offset));

    auto data_result = db_->execute(query, params);
    for (int i = 0; i < data_result.num_rows(); i++) {
        auto row = data_result.row(i);
        query::VehicleSummaryData vs;
        vs.vehicle_id = row.get_string("vehicle_id");
        vs.fleet_id = row.get_string("fleet_id");
        vs.region = row.get_string("region");
        vs.model = row.get_string("model");
        vs.year = row.is_null("year") ? 0 : row.get_int("year");
        vs.owner = row.is_null("owner") ? "" : row.get_string("owner");
        vs.is_online = row.get_string("is_online") == "t";
        vs.created_at_ms = row.is_null("created_at_ms") ? 0 : row.get_int64("created_at_ms");
        vs.updated_at_ms = row.is_null("updated_at_ms") ? 0 : row.get_int64("updated_at_ms");
        result.items.push_back(std::move(vs));
    }

    int next_offset = offset + result.items.size();
    if (next_offset < result.total_count) {
        result.next_page_token = make_page_token(next_offset);
    }

    return result;
}

std::optional<DiscoveryQuery::SyncState> DiscoveryQuery::get_sync_state(
    const std::string& vehicle_id) {

    std::string query = R"(
        SELECT discovery_sequence, discovery_checksum, updated_at
        FROM sync_state
        WHERE vehicle_id = $1
    )";

    auto result = db_->execute(query, {vehicle_id});
    if (result.num_rows() == 0) {
        return std::nullopt;
    }

    SyncState state;
    auto row = result.row(0);
    state.discovery_sequence = row.get_int64("discovery_sequence");
    // discovery_checksum is BIGINT to store uint32 values > INT_MAX
    state.state_checksum = row.is_null("discovery_checksum") ? 0 :
        static_cast<uint32_t>(row.get_int64("discovery_checksum"));
    return state;
}

}  // namespace discovery
}  // namespace cloud
}  // namespace ifex
