#include "discovery_service_impl.hpp"

#include <glog/logging.h>

namespace ifex::cloud::discovery {

CloudDiscoveryServiceImpl::CloudDiscoveryServiceImpl(
    std::shared_ptr<ifex::offboard::PostgresClient> db)
    : db_(db),
      query_(db),
      is_healthy_(true) {
}

grpc::Status CloudDiscoveryServiceImpl::list_vehicles(
    grpc::ServerContext* context,
    const proto::list_vehicles_request* request,
    proto::list_vehicles_response* response) {

    const auto& filter = request->filter();

    VLOG(1) << "list_vehicles: pattern=" << filter.vehicle_id_pattern()
            << " fleet=" << filter.fleet_id_filter()
            << " region=" << filter.region_filter()
            << " page_size=" << filter.page_size();

    try {
        int page_size = filter.page_size();
        if (page_size <= 0 || page_size > 1000) {
            page_size = 100;  // Default
        }

        // Parse page token to get offset
        int offset = 0;
        if (!filter.page_token().empty()) {
            try {
                offset = std::stoi(filter.page_token());
            } catch (...) {
                return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid page token");
            }
        }

        auto result = query_.list_vehicles(
            filter.vehicle_id_pattern(),
            filter.fleet_id_filter(),
            filter.region_filter(),
            filter.online_only(),
            filter.with_services_only(),
            page_size,
            offset);

        // Convert to protobuf
        auto* res = response->mutable_result();
        for (const auto& v : result.items) {
            auto* vehicle = res->add_vehicles();
            vehicle->set_vehicle_id(v.vehicle_id);
            vehicle->set_fleet_id(v.fleet_id);
            vehicle->set_region(v.region);
            vehicle->set_model(v.model);
            vehicle->set_year(v.year);
            vehicle->set_owner(v.owner);
            vehicle->set_service_count(v.service_count);
            vehicle->set_job_count(v.job_count);
            vehicle->set_last_seen_ms(v.last_seen_ms);
            vehicle->set_is_online(v.is_online);
            vehicle->set_created_at_ms(v.created_at_ms);
            vehicle->set_updated_at_ms(v.updated_at_ms);
        }

        res->set_total_count(result.total_count);
        res->set_next_page_token(result.next_page_token);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "list_vehicles failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::get_vehicle_services(
    grpc::ServerContext* context,
    const proto::get_vehicle_services_request* request,
    proto::get_vehicle_services_response* response) {

    if (request->vehicle_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "vehicle_id is required");
    }

    VLOG(1) << "get_vehicle_services: vehicle=" << request->vehicle_id();

    try {
        auto services = query_.get_vehicle_services(request->vehicle_id());

        auto* res = response->mutable_result();
        res->set_vehicle_id(request->vehicle_id());

        for (const auto& s : services) {
            auto* svc = res->add_services();
            svc->set_schema_hash(s.schema_hash);
            svc->set_service_name(s.service_name);
            svc->set_version(s.version);
            svc->set_ifex_schema(s.ifex_schema);
            svc->set_methods_json(s.methods_json);
            svc->set_structs_json(s.structs_json);
            svc->set_enums_json(s.enums_json);
            svc->set_first_seen_ms(s.first_seen_ms);
            svc->set_last_seen_ms(s.last_seen_ms);
        }

        // Get sync state (last_sync_ms)
        auto sync_state = query_.get_sync_state(request->vehicle_id());
        if (sync_state) {
            res->set_last_sync_ms(sync_state->discovery_sequence);
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "get_vehicle_services failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::query_services_by_name(
    grpc::ServerContext* context,
    const proto::query_services_by_name_request* request,
    proto::query_services_by_name_response* response) {

    const auto& filter = request->filter();

    if (filter.service_name_pattern().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name_pattern is required");
    }

    VLOG(1) << "query_services_by_name: pattern=" << filter.service_name_pattern()
            << " fleet=" << filter.fleet_id_filter();

    try {
        int page_size = filter.page_size();
        if (page_size <= 0 || page_size > 1000) {
            page_size = 100;
        }

        int offset = 0;
        if (!filter.page_token().empty()) {
            try {
                offset = std::stoi(filter.page_token());
            } catch (...) {
                return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid page token");
            }
        }

        auto result = query_.query_services_by_name(
            filter.service_name_pattern(),
            filter.fleet_id_filter(),
            filter.region_filter(),
            filter.available_only(),
            page_size,
            offset);

        // Convert to protobuf
        auto* res = response->mutable_result();
        for (const auto& s : result.items) {
            auto* loc = res->add_locations();
            loc->set_vehicle_id(s.vehicle_id);
            loc->set_fleet_id(s.fleet_id);
            loc->set_region(s.region);
            // Set nested service info
            auto* svc = loc->mutable_service();
            svc->set_schema_hash(s.service.schema_hash);
            svc->set_service_name(s.service.service_name);
            svc->set_version(s.service.version);
            svc->set_ifex_schema(s.service.ifex_schema);
            svc->set_methods_json(s.service.methods_json);
            svc->set_structs_json(s.service.structs_json);
            svc->set_enums_json(s.service.enums_json);
            svc->set_first_seen_ms(s.service.first_seen_ms);
            svc->set_last_seen_ms(s.service.last_seen_ms);
        }

        res->set_total_count(result.total_count);
        res->set_next_page_token(result.next_page_token);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "query_services_by_name failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::get_fleet_service_stats(
    grpc::ServerContext* context,
    const proto::get_fleet_service_stats_request* request,
    proto::get_fleet_service_stats_response* response) {

    const auto& filter = request->filter();

    VLOG(1) << "get_fleet_service_stats: fleet=" << filter.fleet_id_filter()
            << " region=" << filter.region_filter();

    try {
        // Get per-service stats
        auto stats = query_.get_fleet_service_stats(
            filter.fleet_id_filter(),
            filter.region_filter());

        auto* res = response->mutable_result();
        for (const auto& s : stats) {
            auto* stat = res->add_stats();
            stat->set_service_name(s.service_name);
            stat->set_vehicle_count(s.vehicle_count);
            stat->set_available_count(s.available_count);
        }

        // Get fleet summary
        auto summary = query_.get_fleet_summary(
            filter.fleet_id_filter(),
            filter.region_filter());

        res->set_total_vehicles(summary.total_vehicles);
        res->set_online_vehicles(summary.online_vehicles);
        res->set_total_services(summary.total_services);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "get_fleet_service_stats failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::find_vehicles_with_service(
    grpc::ServerContext* context,
    const proto::find_vehicles_with_service_request* request,
    proto::find_vehicles_with_service_response* response) {

    const auto& filter = request->filter();

    if (filter.service_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name is required");
    }

    VLOG(1) << "find_vehicles_with_service: service=" << filter.service_name()
            << " method=" << filter.method_name()
            << " fleet=" << filter.fleet_id_filter();

    try {
        int page_size = filter.page_size();
        if (page_size <= 0 || page_size > 1000) {
            page_size = 100;
        }

        int offset = 0;
        if (!filter.page_token().empty()) {
            try {
                offset = std::stoi(filter.page_token());
            } catch (...) {
                return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid page token");
            }
        }

        auto result = query_.find_vehicles_with_service(
            filter.service_name(),
            filter.method_name(),
            filter.fleet_id_filter(),
            filter.region_filter(),
            filter.available_only(),
            page_size,
            offset);

        // Convert to protobuf
        auto* res = response->mutable_result();
        for (const auto& v : result.items) {
            auto* vehicle = res->add_vehicles();
            vehicle->set_vehicle_id(v.vehicle_id);
            vehicle->set_fleet_id(v.fleet_id);
            vehicle->set_region(v.region);
            vehicle->set_model(v.model);
            vehicle->set_year(v.year);
            vehicle->set_owner(v.owner);
            vehicle->set_service_count(v.service_count);
            vehicle->set_job_count(v.job_count);
            vehicle->set_last_seen_ms(v.last_seen_ms);
            vehicle->set_is_online(v.is_online);
            vehicle->set_created_at_ms(v.created_at_ms);
            vehicle->set_updated_at_ms(v.updated_at_ms);
        }

        res->set_total_count(result.total_count);
        res->set_next_page_token(result.next_page_token);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "find_vehicles_with_service failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::healthy(
    grpc::ServerContext* context,
    const proto::healthy_request* request,
    proto::healthy_response* response) {

    // Check if all dependencies are healthy
    bool healthy = is_healthy_ && db_ && db_->is_connected();

    response->set_is_healthy(healthy);

    return grpc::Status::OK;
}

}  // namespace ifex::cloud::discovery
