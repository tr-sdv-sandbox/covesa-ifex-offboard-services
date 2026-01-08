#include "dispatcher_service_impl.hpp"

#include <glog/logging.h>
#include <chrono>
#include <thread>

#include "dispatcher-rpc-envelope.pb.h"

namespace ifex {
namespace cloud {
namespace dispatcher {

CloudDispatcherServiceImpl::CloudDispatcherServiceImpl(
    std::shared_ptr<RpcRequestManager> request_manager,
    std::shared_ptr<RequestProducer> producer)
    : request_manager_(std::move(request_manager)),
      producer_(std::move(producer)) {}

std::string CloudDispatcherServiceImpl::build_request_payload(
    const std::string& correlation_id,
    const std::string& vehicle_id,
    const std::string& service_name,
    const std::string& method_name,
    const std::string& parameters_json,
    uint32_t timeout_ms) {

    swdv::dispatcher_rpc_envelope::rpc_request_t request;
    request.set_correlation_id(correlation_id);
    request.set_service_name(service_name);
    request.set_method_name(method_name);
    request.set_parameters_json(parameters_json);
    request.set_timeout_ms(timeout_ms);

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    request.set_request_timestamp_ms(now_ms);

    return request.SerializeAsString();
}

grpc::Status CloudDispatcherServiceImpl::CallMethod(
    grpc::ServerContext* context,
    const CallMethodRequest* request,
    CallMethodResponse* response) {

    // DEPRECATED: Sync CallMethod blocks a gRPC thread. Use CallMethodAsync + GetRpcStatus.
    LOG(WARNING) << "CallMethod (sync) is deprecated for scalability. Use CallMethodAsync.";

    if (request->vehicle_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "vehicle_id is required");
    }
    if (request->service_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name is required");
    }
    if (request->method_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method_name is required");
    }

    int timeout_ms = request->timeout_ms();
    if (timeout_ms <= 0) {
        timeout_ms = 30000;  // Default 30 seconds
    }

    LOG(INFO) << "CallMethod: vehicle=" << request->vehicle_id()
              << " method=" << request->service_name() << "." << request->method_name()
              << " timeout=" << timeout_ms << "ms";

    try {
        // Create request and track it
        std::string correlation_id = request_manager_->create_request(
            request->vehicle_id(),
            request->service_name(),
            request->method_name(),
            request->parameters_json(),
            request->requester_id(),
            std::chrono::milliseconds(timeout_ms));

        // Build and send the request payload
        std::string payload = build_request_payload(
            correlation_id,
            request->vehicle_id(),
            request->service_name(),
            request->method_name(),
            request->parameters_json(),
            timeout_ms);

        if (!producer_->send(request->vehicle_id(), payload)) {
            return grpc::Status(grpc::INTERNAL, "Failed to send request to Kafka");
        }

        // Poll for response (blocking, but no condition_variable needed)
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        constexpr auto poll_interval = std::chrono::milliseconds(50);

        while (std::chrono::steady_clock::now() < deadline) {
            auto info = request_manager_->get_request(correlation_id);
            if (info && info->status != RpcStatus::PENDING && info->status != RpcStatus::IN_PROGRESS) {
                // Request completed
                response->set_correlation_id(correlation_id);

                switch (info->status) {
                    case RpcStatus::SUCCESS:
                        response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_SUCCESS);
                        break;
                    case RpcStatus::TIMEOUT:
                        response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_TIMEOUT);
                        break;
                    case RpcStatus::CANCELLED:
                        response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_CANCELLED);
                        break;
                    case RpcStatus::ERROR:
                    default:
                        response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_FAILED);
                        break;
                }

                response->set_result_json(info->result_json);
                response->set_error_message(info->error_message);
                response->set_duration_ms(info->execution_time_ms);

                return grpc::Status::OK;
            }

            std::this_thread::sleep_for(poll_interval);
        }

        // Timeout
        response->set_correlation_id(correlation_id);
        response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_TIMEOUT);
        response->set_error_message("Request timed out");

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "CallMethod failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::CallMethodAsync(
    grpc::ServerContext* context,
    const CallMethodAsyncRequest* request,
    CallMethodAsyncResponse* response) {

    if (request->vehicle_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "vehicle_id is required");
    }
    if (request->service_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name is required");
    }
    if (request->method_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method_name is required");
    }

    int timeout_ms = request->timeout_ms();
    if (timeout_ms <= 0) {
        timeout_ms = 30000;
    }

    LOG(INFO) << "CallMethodAsync: vehicle=" << request->vehicle_id()
              << " method=" << request->service_name() << "." << request->method_name();

    try {
        // Create request and track it
        std::string correlation_id = request_manager_->create_request(
            request->vehicle_id(),
            request->service_name(),
            request->method_name(),
            request->parameters_json(),
            request->requester_id(),
            std::chrono::milliseconds(timeout_ms));

        // Build and send the request payload
        std::string payload = build_request_payload(
            correlation_id,
            request->vehicle_id(),
            request->service_name(),
            request->method_name(),
            request->parameters_json(),
            timeout_ms);

        if (!producer_->send(request->vehicle_id(), payload)) {
            return grpc::Status(grpc::INTERNAL, "Failed to send request to Kafka");
        }

        // Return immediately with correlation_id
        response->set_correlation_id(correlation_id);
        response->set_accepted(true);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "CallMethodAsync failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::GetRpcStatus(
    grpc::ServerContext* context,
    const GetRpcStatusRequest* request,
    GetRpcStatusResponse* response) {

    if (request->correlation_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "correlation_id is required");
    }

    try {
        auto info = request_manager_->get_request(request->correlation_id());
        if (!info) {
            return grpc::Status(grpc::NOT_FOUND, "Request not found");
        }

        response->set_correlation_id(info->correlation_id);
        response->set_vehicle_id(info->vehicle_id);
        response->set_service_name(info->service_name);
        response->set_method_name(info->method_name);

        switch (info->status) {
            case RpcStatus::PENDING:
            case RpcStatus::IN_PROGRESS:
                response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_PENDING);
                break;
            case RpcStatus::SUCCESS:
                response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_SUCCESS);
                break;
            case RpcStatus::TIMEOUT:
                response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_TIMEOUT);
                break;
            case RpcStatus::CANCELLED:
                response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_CANCELLED);
                break;
            case RpcStatus::ERROR:
            default:
                response->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_FAILED);
                break;
        }

        response->set_result_json(info->result_json);
        response->set_error_message(info->error_message);
        response->set_created_at_ms(info->created_at_ms);
        response->set_responded_at_ms(info->completed_at_ms);
        response->set_duration_ms(info->execution_time_ms);
        response->set_completed(info->status != RpcStatus::PENDING &&
                                info->status != RpcStatus::IN_PROGRESS);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "GetRpcStatus failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::ListRpcRequests(
    grpc::ServerContext* context,
    const ListRpcRequestsRequest* request,
    ListRpcRequestsResponse* response) {

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

    try {
        // Map filter status based on pending_only/failed_only flags
        RpcStatus filter_status = RpcStatus::PENDING;
        bool filter_by_status = false;
        if (request->pending_only()) {
            filter_status = RpcStatus::PENDING;
            filter_by_status = true;
        } else if (request->failed_only()) {
            filter_status = RpcStatus::ERROR;
            filter_by_status = true;
        }

        auto requests = request_manager_->list_requests(
            request->vehicle_id_filter(),
            request->service_name_filter(),
            filter_by_status ? filter_status : RpcStatus::PENDING,
            page_size,
            offset);

        for (const auto& info : requests) {
            auto* req = response->add_requests();
            req->set_correlation_id(info.correlation_id);
            req->set_vehicle_id(info.vehicle_id);
            req->set_service_name(info.service_name);
            req->set_method_name(info.method_name);
            req->set_requester_id(info.requester_id);

            switch (info.status) {
                case RpcStatus::PENDING:
                case RpcStatus::IN_PROGRESS:
                    req->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_PENDING);
                    break;
                case RpcStatus::SUCCESS:
                    req->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_SUCCESS);
                    break;
                case RpcStatus::TIMEOUT:
                    req->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_TIMEOUT);
                    break;
                case RpcStatus::CANCELLED:
                    req->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_CANCELLED);
                    break;
                default:
                    req->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_FAILED);
                    break;
            }

            req->set_created_at_ms(info.created_at_ms);
            req->set_responded_at_ms(info.completed_at_ms);
        }

        if (static_cast<int>(requests.size()) == page_size) {
            response->set_next_page_token(std::to_string(offset + page_size));
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "ListRpcRequests failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::CancelRpc(
    grpc::ServerContext* context,
    const CancelRpcRequest* request,
    CancelRpcResponse* response) {

    if (request->correlation_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "correlation_id is required");
    }

    try {
        bool cancelled = request_manager_->cancel_request(request->correlation_id());
        response->set_success(cancelled);

        if (!cancelled) {
            response->set_error_message("Request not found or already completed");
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "CancelRpc failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::CallFleetMethod(
    grpc::ServerContext* context,
    const CallFleetMethodRequest* request,
    CallFleetMethodResponse* response) {

    if (request->vehicle_ids().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "At least one vehicle_id is required");
    }
    if (request->service_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name is required");
    }
    if (request->method_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method_name is required");
    }

    int timeout_ms = request->timeout_ms();
    if (timeout_ms <= 0) {
        timeout_ms = 30000;
    }

    LOG(INFO) << "CallFleetMethod: " << request->vehicle_ids().size() << " vehicles"
              << " method=" << request->service_name() << "." << request->method_name();

    try {
        // Send requests to all vehicles
        for (const auto& vehicle_id : request->vehicle_ids()) {
            std::string correlation_id = request_manager_->create_request(
                vehicle_id,
                request->service_name(),
                request->method_name(),
                request->parameters_json(),
                request->requester_id(),
                std::chrono::milliseconds(timeout_ms));

            std::string payload = build_request_payload(
                correlation_id,
                vehicle_id,
                request->service_name(),
                request->method_name(),
                request->parameters_json(),
                timeout_ms);

            auto* result = response->add_results();
            result->set_vehicle_id(vehicle_id);
            result->set_correlation_id(correlation_id);
            if (producer_->send(vehicle_id, payload)) {
                result->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_PENDING);
            } else {
                result->set_status(::ifex::cloud::dispatcher::CLOUD_RPC_TRANSPORT_ERROR);
                result->set_error_message("Failed to send to Kafka");
            }
        }

        // Count successes and failures
        int successful = 0, failed = 0;
        for (const auto& r : response->results()) {
            if (r.status() == ::ifex::cloud::dispatcher::CLOUD_RPC_PENDING) {
                successful++;
            } else {
                failed++;
            }
        }
        response->set_total_vehicles(response->results_size());
        response->set_successful(successful);
        response->set_failed(failed);
        response->set_pending(successful);  // All successful are pending

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "CallFleetMethod failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

}  // namespace dispatcher
}  // namespace cloud
}  // namespace ifex
