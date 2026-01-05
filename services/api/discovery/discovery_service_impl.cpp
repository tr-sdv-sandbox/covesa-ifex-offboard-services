#include "discovery_service_impl.hpp"

#include <glog/logging.h>

namespace ifex {
namespace cloud {
namespace discovery {

CloudDiscoveryServiceImpl::CloudDiscoveryServiceImpl(
    std::shared_ptr<ifex::offboard::PostgresClient> db)
    : query_(std::move(db)) {
}

grpc::Status CloudDiscoveryServiceImpl::ListVehicles(
    grpc::ServerContext* context,
    const ListVehiclesRequest* request,
    ListVehiclesResponse* response) {

    VLOG(1) << "ListVehicles: fleet=" << request->fleet_id_filter()
            << " region=" << request->region_filter()
            << " page_size=" << request->page_size();

    try {
        int page_size = request->page_size();
        if (page_size <= 0 || page_size > 1000) {
            page_size = 100;  // Default
        }

        // Parse page token to get offset
        int offset = 0;
        if (!request->page_token().empty()) {
            try {
                offset = std::stoi(request->page_token());
            } catch (...) {
                return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid page token");
            }
        }

        auto result = query_.list_vehicles(
            request->fleet_id_filter(),
            request->region_filter(),
            request->online_only(),
            request->with_services_only(),
            page_size,
            offset);

        // Convert to protobuf
        for (const auto& v : result.items) {
            auto* vehicle = response->add_vehicles();
            vehicle->set_vehicle_id(v.vehicle_id);
            vehicle->set_fleet_id(v.fleet_id);
            vehicle->set_region(v.region);
            vehicle->set_model(v.model);
            vehicle->set_year(v.year);
            vehicle->set_service_count(v.service_count);
            vehicle->set_job_count(v.job_count);
            vehicle->set_last_seen_ns(v.last_seen_ns);
            vehicle->set_is_online(v.is_online);
        }

        response->set_total_count(result.total_count);
        response->set_next_page_token(result.next_page_token);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "ListVehicles failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::GetVehicleServices(
    grpc::ServerContext* context,
    const GetVehicleServicesRequest* request,
    GetVehicleServicesResponse* response) {

    if (request->vehicle_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "vehicle_id is required");
    }

    VLOG(1) << "GetVehicleServices: vehicle=" << request->vehicle_id();

    try {
        auto services = query_.get_vehicle_services(request->vehicle_id());

        for (const auto& s : services) {
            auto* svc = response->add_services();
            svc->set_name(s.service_name);
            svc->set_version(s.version);
            svc->mutable_endpoint()->set_address(s.endpoint_address);
            // Map transport_type string to enum
            if (s.transport_type == "grpc") {
                svc->mutable_endpoint()->set_transport(swdv::discovery_sync_envelope::GRPC);
            } else if (s.transport_type == "http_rest") {
                svc->mutable_endpoint()->set_transport(swdv::discovery_sync_envelope::HTTP_REST);
            } else if (s.transport_type == "someip") {
                svc->mutable_endpoint()->set_transport(swdv::discovery_sync_envelope::SOMEIP);
            }
            svc->set_status(s.status);
            svc->set_last_heartbeat_ms(s.last_heartbeat_ms);
        }

        // Get sync state
        auto sync_state = query_.get_sync_state(request->vehicle_id());
        if (sync_state) {
            response->set_last_sync_ns(sync_state->discovery_sequence);
            response->set_state_checksum(sync_state->state_checksum);
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "GetVehicleServices failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::QueryServicesByName(
    grpc::ServerContext* context,
    const QueryServicesByNameRequest* request,
    QueryServicesByNameResponse* response) {

    if (request->service_name_pattern().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name_pattern is required");
    }

    VLOG(1) << "QueryServicesByName: pattern=" << request->service_name_pattern()
            << " fleet=" << request->fleet_id_filter();

    try {
        int page_size = request->page_size();
        if (page_size <= 0 || page_size > 1000) {
            page_size = 100;
        }

        int offset = 0;
        if (!request->page_token().empty()) {
            try {
                offset = std::stoi(request->page_token());
            } catch (...) {
                return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid page token");
            }
        }

        auto result = query_.query_services_by_name(
            request->service_name_pattern(),
            request->fleet_id_filter(),
            request->region_filter(),
            request->available_only(),
            page_size,
            offset);

        // Convert to protobuf
        for (const auto& s : result.items) {
            auto* loc = response->add_locations();
            loc->set_vehicle_id(s.vehicle_id);
            loc->set_fleet_id(s.fleet_id);
            loc->set_region(s.region);
            // Set nested service info
            auto* svc = loc->mutable_service();
            svc->set_name(s.service_name);
            svc->set_version(s.version);
            svc->mutable_endpoint()->set_address(s.endpoint_address);
            svc->set_status(s.status);
            svc->set_last_heartbeat_ms(s.last_heartbeat_ms);
        }

        response->set_total_count(result.total_count);
        response->set_next_page_token(result.next_page_token);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "QueryServicesByName failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::GetFleetServiceStats(
    grpc::ServerContext* context,
    const GetFleetServiceStatsRequest* request,
    GetFleetServiceStatsResponse* response) {

    VLOG(1) << "GetFleetServiceStats: fleet=" << request->fleet_id_filter()
            << " region=" << request->region_filter();

    try {
        // Get per-service stats
        auto stats = query_.get_fleet_service_stats(
            request->fleet_id_filter(),
            request->region_filter());

        for (const auto& s : stats) {
            auto* stat = response->add_stats();
            stat->set_service_name(s.service_name);
            stat->set_vehicle_count(s.vehicle_count);
            stat->set_available_count(s.available_count);

            for (const auto& [version, count] : s.by_version) {
                (*stat->mutable_by_version())[version] = count;
            }
            for (const auto& [region, count] : s.by_region) {
                (*stat->mutable_by_region())[region] = count;
            }
        }

        // Get fleet summary
        auto summary = query_.get_fleet_summary(
            request->fleet_id_filter(),
            request->region_filter());

        response->set_total_vehicles(summary.total_vehicles);
        response->set_online_vehicles(summary.online_vehicles);
        response->set_total_services(summary.total_services);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "GetFleetServiceStats failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDiscoveryServiceImpl::FindVehiclesWithService(
    grpc::ServerContext* context,
    const FindVehiclesWithServiceRequest* request,
    FindVehiclesWithServiceResponse* response) {

    if (request->service_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name is required");
    }

    VLOG(1) << "FindVehiclesWithService: service=" << request->service_name()
            << " method=" << request->method_name()
            << " fleet=" << request->fleet_id_filter();

    try {
        int page_size = request->page_size();
        if (page_size <= 0 || page_size > 1000) {
            page_size = 100;
        }

        int offset = 0;
        if (!request->page_token().empty()) {
            try {
                offset = std::stoi(request->page_token());
            } catch (...) {
                return grpc::Status(grpc::INVALID_ARGUMENT, "Invalid page token");
            }
        }

        auto result = query_.find_vehicles_with_service(
            request->service_name(),
            request->method_name(),
            request->fleet_id_filter(),
            request->region_filter(),
            request->available_only(),
            page_size,
            offset);

        // Convert to protobuf
        for (const auto& v : result.items) {
            auto* vehicle = response->add_vehicles();
            vehicle->set_vehicle_id(v.vehicle_id);
            vehicle->set_fleet_id(v.fleet_id);
            vehicle->set_region(v.region);
            vehicle->set_model(v.model);
            vehicle->set_year(v.year);
            vehicle->set_service_count(v.service_count);
            vehicle->set_job_count(v.job_count);
            vehicle->set_last_seen_ns(v.last_seen_ns);
            vehicle->set_is_online(v.is_online);
        }

        response->set_total_count(result.total_count);
        response->set_next_page_token(result.next_page_token);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "FindVehiclesWithService failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

}  // namespace discovery
}  // namespace cloud
}  // namespace ifex
