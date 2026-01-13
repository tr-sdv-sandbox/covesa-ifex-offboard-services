#pragma once

#include <memory>

#include "cloud-discovery-service.grpc.pb.h"
#include "discovery_query.hpp"

namespace ifex::cloud::discovery {

// Namespace aliases for IFEX-generated types
namespace proto = swdv::cloud_discovery_service;

/**
 * Cloud Discovery Service gRPC implementation (IFEX-based)
 *
 * Read-only service that queries PostgreSQL for fleet-wide
 * service registry information.
 * Inherits from multiple IFEX-style service classes (one per method).
 */
class CloudDiscoveryServiceImpl final
    : public proto::list_vehicles_service::Service,
      public proto::get_vehicle_services_service::Service,
      public proto::query_services_by_name_service::Service,
      public proto::get_fleet_service_stats_service::Service,
      public proto::find_vehicles_with_service_service::Service,
      public proto::healthy_service::Service {
public:
    explicit CloudDiscoveryServiceImpl(
        std::shared_ptr<ifex::offboard::PostgresClient> db);

    // List vehicles with optional filters
    grpc::Status list_vehicles(
        grpc::ServerContext* context,
        const proto::list_vehicles_request* request,
        proto::list_vehicles_response* response) override;

    // Get services for a specific vehicle
    grpc::Status get_vehicle_services(
        grpc::ServerContext* context,
        const proto::get_vehicle_services_request* request,
        proto::get_vehicle_services_response* response) override;

    // Query services by name pattern
    grpc::Status query_services_by_name(
        grpc::ServerContext* context,
        const proto::query_services_by_name_request* request,
        proto::query_services_by_name_response* response) override;

    // Get fleet-wide service statistics
    grpc::Status get_fleet_service_stats(
        grpc::ServerContext* context,
        const proto::get_fleet_service_stats_request* request,
        proto::get_fleet_service_stats_response* response) override;

    // Find vehicles that have a specific service
    grpc::Status find_vehicles_with_service(
        grpc::ServerContext* context,
        const proto::find_vehicles_with_service_request* request,
        proto::find_vehicles_with_service_response* response) override;

    // Health check (IFEX standard method)
    grpc::Status healthy(
        grpc::ServerContext* context,
        const proto::healthy_request* request,
        proto::healthy_response* response) override;

private:
    std::shared_ptr<ifex::offboard::PostgresClient> db_;
    DiscoveryQuery query_;
    bool is_healthy_ = false;
};

}  // namespace ifex::cloud::discovery
