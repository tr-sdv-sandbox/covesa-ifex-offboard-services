#pragma once

#include <memory>

#include "cloud-dispatcher-service.grpc.pb.h"
#include "rpc_request_manager.hpp"
#include "request_producer.hpp"

namespace ifex::cloud::dispatcher {

// Namespace aliases for IFEX-generated types
namespace proto = swdv::cloud_dispatcher_service;

/**
 * Cloud Dispatcher Service gRPC implementation (IFEX-based)
 *
 * Routes RPC requests to vehicles via Kafka and tracks responses in-memory.
 * Inherits from multiple IFEX-style service classes (one per method).
 */
class CloudDispatcherServiceImpl final
    : public proto::call_method_service::Service,
      public proto::call_method_async_service::Service,
      public proto::get_rpc_status_service::Service,
      public proto::list_rpc_requests_service::Service,
      public proto::cancel_rpc_service::Service,
      public proto::call_fleet_method_service::Service,
      public proto::healthy_service::Service {
public:
    CloudDispatcherServiceImpl(
        std::shared_ptr<RpcRequestManager> request_manager,
        std::shared_ptr<RequestProducer> producer);

    // Synchronous RPC call (waits for response)
    grpc::Status call_method(
        grpc::ServerContext* context,
        const proto::call_method_request* request,
        proto::call_method_response* response) override;

    // Asynchronous RPC call (returns immediately with correlation_id)
    grpc::Status call_method_async(
        grpc::ServerContext* context,
        const proto::call_method_async_request* request,
        proto::call_method_async_response* response) override;

    // Get status of an async RPC
    grpc::Status get_rpc_status(
        grpc::ServerContext* context,
        const proto::get_rpc_status_request* request,
        proto::get_rpc_status_response* response) override;

    // List recent RPC requests
    grpc::Status list_rpc_requests(
        grpc::ServerContext* context,
        const proto::list_rpc_requests_request* request,
        proto::list_rpc_requests_response* response) override;

    // Cancel a pending RPC
    grpc::Status cancel_rpc(
        grpc::ServerContext* context,
        const proto::cancel_rpc_request* request,
        proto::cancel_rpc_response* response) override;

    // Call method on multiple vehicles
    grpc::Status call_fleet_method(
        grpc::ServerContext* context,
        const proto::call_fleet_method_request* request,
        proto::call_fleet_method_response* response) override;

    // Health check (IFEX standard method)
    grpc::Status healthy(
        grpc::ServerContext* context,
        const proto::healthy_request* request,
        proto::healthy_response* response) override;

private:
    // Build the RPC request payload for Kafka
    std::string build_request_payload(
        const std::string& correlation_id,
        const std::string& vehicle_id,
        const std::string& service_name,
        const std::string& method_name,
        const std::string& parameters_json,
        uint32_t timeout_ms);

    // Map internal RpcStatus to IFEX cloud_rpc_status_t
    proto::cloud_rpc_status_t map_status(RpcStatus status);

    std::shared_ptr<RpcRequestManager> request_manager_;
    std::shared_ptr<RequestProducer> producer_;
    bool is_healthy_ = false;
};

}  // namespace ifex::cloud::dispatcher
