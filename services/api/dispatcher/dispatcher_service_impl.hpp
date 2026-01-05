#pragma once

#include <memory>

#include "cloud-dispatcher-service.grpc.pb.h"
#include "rpc_request_manager.hpp"
#include "request_producer.hpp"
#include "postgres_client.hpp"

namespace ifex {
namespace cloud {
namespace dispatcher {

/**
 * Cloud Dispatcher Service gRPC implementation
 *
 * Routes RPC requests to vehicles via Kafka and tracks responses.
 */
class CloudDispatcherServiceImpl final
    : public ifex::cloud::dispatcher::CloudDispatcherService::Service {
public:
    CloudDispatcherServiceImpl(
        std::shared_ptr<ifex::offboard::PostgresClient> db,
        std::shared_ptr<RpcRequestManager> request_manager,
        std::shared_ptr<RequestProducer> producer);

    // Synchronous RPC call (waits for response)
    grpc::Status CallMethod(
        grpc::ServerContext* context,
        const CallMethodRequest* request,
        CallMethodResponse* response) override;

    // Asynchronous RPC call (returns immediately with correlation_id)
    grpc::Status CallMethodAsync(
        grpc::ServerContext* context,
        const CallMethodAsyncRequest* request,
        CallMethodAsyncResponse* response) override;

    // Get status of an async RPC
    grpc::Status GetRpcStatus(
        grpc::ServerContext* context,
        const GetRpcStatusRequest* request,
        GetRpcStatusResponse* response) override;

    // List recent RPC requests
    grpc::Status ListRpcRequests(
        grpc::ServerContext* context,
        const ListRpcRequestsRequest* request,
        ListRpcRequestsResponse* response) override;

    // Cancel a pending RPC
    grpc::Status CancelRpc(
        grpc::ServerContext* context,
        const CancelRpcRequest* request,
        CancelRpcResponse* response) override;

    // Call method on multiple vehicles
    grpc::Status CallFleetMethod(
        grpc::ServerContext* context,
        const CallFleetMethodRequest* request,
        CallFleetMethodResponse* response) override;

private:
    // Build the RPC request payload for Kafka
    std::string build_request_payload(
        const std::string& correlation_id,
        const std::string& vehicle_id,
        const std::string& service_name,
        const std::string& method_name,
        const std::string& parameters_json,
        uint32_t timeout_ms);

    std::shared_ptr<ifex::offboard::PostgresClient> db_;
    std::shared_ptr<RpcRequestManager> request_manager_;
    std::shared_ptr<RequestProducer> producer_;
};

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
