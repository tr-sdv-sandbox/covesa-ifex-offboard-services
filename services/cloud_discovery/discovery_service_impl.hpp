#pragma once

#include <memory>

#include "cloud-discovery-service.grpc.pb.h"
#include "discovery_query.hpp"

namespace ifex {
namespace cloud {
namespace discovery {

/**
 * Cloud Discovery Service gRPC implementation
 *
 * Read-only service that queries PostgreSQL for fleet-wide
 * service registry information.
 */
class CloudDiscoveryServiceImpl final
    : public ifex::cloud::discovery::CloudDiscoveryService::Service {
public:
    explicit CloudDiscoveryServiceImpl(
        std::shared_ptr<ifex::offboard::PostgresClient> db);

    // List vehicles with optional filters
    grpc::Status ListVehicles(
        grpc::ServerContext* context,
        const ListVehiclesRequest* request,
        ListVehiclesResponse* response) override;

    // Get services for a specific vehicle
    grpc::Status GetVehicleServices(
        grpc::ServerContext* context,
        const GetVehicleServicesRequest* request,
        GetVehicleServicesResponse* response) override;

    // Query services by name pattern
    grpc::Status QueryServicesByName(
        grpc::ServerContext* context,
        const QueryServicesByNameRequest* request,
        QueryServicesByNameResponse* response) override;

    // Get fleet-wide service statistics
    grpc::Status GetFleetServiceStats(
        grpc::ServerContext* context,
        const GetFleetServiceStatsRequest* request,
        GetFleetServiceStatsResponse* response) override;

    // Find vehicles that have a specific service
    grpc::Status FindVehiclesWithService(
        grpc::ServerContext* context,
        const FindVehiclesWithServiceRequest* request,
        FindVehiclesWithServiceResponse* response) override;

private:
    DiscoveryQuery query_;
};

}  // namespace discovery
}  // namespace cloud
}  // namespace ifex
