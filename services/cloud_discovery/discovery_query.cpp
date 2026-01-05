#include "discovery_query.hpp"

#include <algorithm>
#include <sstream>

#include <glog/logging.h>

namespace ifex {
namespace cloud {
namespace discovery {

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
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as service_count
            FROM services
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
            COALESCE(s.service_count, 0) as service_count,
            COALESCE(j.job_count, 0) as job_count,
            v.last_seen_at,
            v.is_online
        FROM vehicle_enrichment e
        LEFT JOIN vehicles v ON e.vehicle_id = v.vehicle_id
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as service_count
            FROM services
            GROUP BY vehicle_id
        ) s ON e.vehicle_id = s.vehicle_id
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as job_count
            FROM jobs
            GROUP BY vehicle_id
        ) j ON e.vehicle_id = j.vehicle_id
    )" + where_clause + R"(
        ORDER BY e.vehicle_id
        LIMIT $)" + std::to_string(param_idx++) + R"(
        OFFSET $)" + std::to_string(param_idx++);

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
        vs.year = row.get_int("year");
        vs.service_count = row.get_int("service_count");
        vs.job_count = row.get_int("job_count");
        vs.is_online = row.get_string("is_online") == "t";
        result.items.push_back(std::move(vs));
    }

    // Set next page token if there are more results
    int next_offset = offset + result.items.size();
    if (next_offset < result.total_count) {
        result.next_page_token = make_page_token(next_offset);
    }

    return result;
}

std::vector<query::ServiceLocationData> DiscoveryQuery::get_vehicle_services(
    const std::string& vehicle_id) {

    std::vector<query::ServiceLocationData> services;

    std::string query = R"(
        SELECT
            s.vehicle_id,
            e.fleet_id,
            e.region,
            s.service_name,
            s.version,
            s.endpoint_address,
            s.transport_type,
            s.status,
            s.last_heartbeat_ms
        FROM services s
        JOIN vehicle_enrichment e ON s.vehicle_id = e.vehicle_id
        WHERE s.vehicle_id = $1
        ORDER BY s.service_name
    )";

    auto result = db_->execute(query, {vehicle_id});
    for (int i = 0; i < result.num_rows(); i++) {
        auto row = result.row(i);
        query::ServiceLocationData sl;
        sl.vehicle_id = row.get_string("vehicle_id");
        sl.fleet_id = row.get_string("fleet_id");
        sl.region = row.get_string("region");
        sl.service_name = row.get_string("service_name");
        sl.version = row.get_string("version");
        sl.endpoint_address = row.get_string("endpoint_address");
        sl.transport_type = row.get_string("transport_type");
        sl.status = row.get_int("status");
        sl.last_heartbeat_ms = row.get_int64("last_heartbeat_ms");
        services.push_back(std::move(sl));
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
    conditions.push_back("s.service_name LIKE $" + std::to_string(param_idx++));
    params.push_back(to_sql_pattern(name_pattern));

    if (!fleet_id_filter.empty()) {
        conditions.push_back("e.fleet_id = $" + std::to_string(param_idx++));
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        conditions.push_back("e.region = $" + std::to_string(param_idx++));
        params.push_back(region_filter);
    }
    if (available_only) {
        conditions.push_back("s.status = 0");  // AVAILABLE = 0
    }

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
        FROM services s
        JOIN vehicle_enrichment e ON s.vehicle_id = e.vehicle_id
    )" + where_clause;

    auto count_result = db_->execute(count_query, params);
    if (count_result.num_rows() > 0) {
        result.total_count = count_result.row(0).get_int("count");
    }

    // Data query
    std::string query = R"(
        SELECT
            s.vehicle_id,
            e.fleet_id,
            e.region,
            s.service_name,
            s.version,
            s.endpoint_address,
            s.transport_type,
            s.status,
            s.last_heartbeat_ms
        FROM services s
        JOIN vehicle_enrichment e ON s.vehicle_id = e.vehicle_id
    )" + where_clause + R"(
        ORDER by s.service_name, s.vehicle_id
        LIMIT $)" + std::to_string(param_idx++) + R"(
        OFFSET $)" + std::to_string(param_idx++);

    params.push_back(std::to_string(page_size));
    params.push_back(std::to_string(offset));

    auto data_result = db_->execute(query, params);
    for (int i = 0; i < data_result.num_rows(); i++) {
        auto row = data_result.row(i);
        query::ServiceLocationData sl;
        sl.vehicle_id = row.get_string("vehicle_id");
        sl.fleet_id = row.get_string("fleet_id");
        sl.region = row.get_string("region");
        sl.service_name = row.get_string("service_name");
        sl.version = row.get_string("version");
        sl.endpoint_address = row.get_string("endpoint_address");
        sl.transport_type = row.get_string("transport_type");
        sl.status = row.get_int("status");
        sl.last_heartbeat_ms = row.get_int64("last_heartbeat_ms");
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
            s.service_name,
            COUNT(DISTINCT s.vehicle_id) as vehicle_count,
            COUNT(DISTINCT CASE WHEN s.status = 0 THEN s.vehicle_id END) as available_count
        FROM services s
        JOIN vehicle_enrichment e ON s.vehicle_id = e.vehicle_id
    )" + where_clause + R"(
        GROUP BY s.service_name
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
        SELECT COUNT(DISTINCT s.vehicle_id)
        FROM services s
        JOIN vehicle_enrichment e ON s.vehicle_id = e.vehicle_id
    )" + where_clause;
    result = db_->execute(online_query, params);
    if (result.num_rows() > 0) {
        summary.online_vehicles = result.row(0).get_int("count");
    }

    // Total services
    std::string services_query = R"(
        SELECT COUNT(*)
        FROM services s
        JOIN vehicle_enrichment e ON s.vehicle_id = e.vehicle_id
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

    conditions.push_back("s.service_name = $" + std::to_string(param_idx++));
    params.push_back(service_name);

    if (!fleet_id_filter.empty()) {
        conditions.push_back("e.fleet_id = $" + std::to_string(param_idx++));
        params.push_back(fleet_id_filter);
    }
    if (!region_filter.empty()) {
        conditions.push_back("e.region = $" + std::to_string(param_idx++));
        params.push_back(region_filter);
    }
    if (available_only) {
        conditions.push_back("s.status = 0");
    }

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
        SELECT COUNT(DISTINCT s.vehicle_id)
        FROM services s
        JOIN vehicle_enrichment e ON s.vehicle_id = e.vehicle_id
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
            e.year
        FROM vehicle_enrichment e
        JOIN services s ON e.vehicle_id = s.vehicle_id
    )" + where_clause + R"(
        ORDER BY e.vehicle_id
        LIMIT $)" + std::to_string(param_idx++) + R"(
        OFFSET $)" + std::to_string(param_idx++);

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
        vs.year = row.get_int("year");
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
    state.state_checksum = static_cast<uint32_t>(row.get_int("discovery_checksum"));
    return state;
}

}  // namespace discovery
}  // namespace cloud
}  // namespace ifex
