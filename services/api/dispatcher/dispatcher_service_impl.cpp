#include "dispatcher_service_impl.hpp"

#include <glog/logging.h>
#include <chrono>
#include <thread>

#include "dispatcher-rpc-envelope.pb.h"

namespace ifex::cloud::dispatcher {

CloudDispatcherServiceImpl::CloudDispatcherServiceImpl(
    std::shared_ptr<RpcRequestManager> request_manager,
    std::shared_ptr<RequestProducer> producer)
    : request_manager_(std::move(request_manager)),
      producer_(std::move(producer)),
      is_healthy_(true) {}

proto::cloud_rpc_status_t CloudDispatcherServiceImpl::map_status(RpcStatus status) {
    switch (status) {
        case RpcStatus::PENDING:
        case RpcStatus::IN_PROGRESS:
            return proto::RPC_PENDING;
        case RpcStatus::SUCCESS:
            return proto::RPC_SUCCESS;
        case RpcStatus::TIMEOUT:
            return proto::RPC_TIMEOUT;
        case RpcStatus::CANCELLED:
            return proto::RPC_CANCELLED;
        case RpcStatus::ERROR:
        default:
            return proto::RPC_FAILED;
    }
}

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

grpc::Status CloudDispatcherServiceImpl::call_method(
    grpc::ServerContext* context,
    const proto::call_method_request* request,
    proto::call_method_response* response) {

    const auto& req = request->request();

    // DEPRECATED: Sync call_method blocks a gRPC thread. Use call_method_async + get_rpc_status.
    LOG(WARNING) << "call_method (sync) is deprecated for scalability. Use call_method_async.";

    if (req.vehicle_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "vehicle_id is required");
    }
    if (req.service_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name is required");
    }
    if (req.method_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method_name is required");
    }

    int timeout_ms = req.timeout_ms();
    if (timeout_ms <= 0) {
        timeout_ms = 30000;  // Default 30 seconds
    }

    LOG(INFO) << "call_method: vehicle=" << req.vehicle_id()
              << " method=" << req.service_name() << "." << req.method_name()
              << " timeout=" << timeout_ms << "ms";

    try {
        // Create request and track it
        std::string correlation_id = request_manager_->create_request(
            req.vehicle_id(),
            req.service_name(),
            req.method_name(),
            req.parameters_json(),
            req.requester_id(),
            std::chrono::milliseconds(timeout_ms));

        // Build and send the request payload
        std::string payload = build_request_payload(
            correlation_id,
            req.vehicle_id(),
            req.service_name(),
            req.method_name(),
            req.parameters_json(),
            timeout_ms);

        if (!producer_->send(req.vehicle_id(), payload)) {
            return grpc::Status(grpc::INTERNAL, "Failed to send request to Kafka");
        }

        // Poll for response (blocking, but no condition_variable needed)
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
        constexpr auto poll_interval = std::chrono::milliseconds(50);

        while (std::chrono::steady_clock::now() < deadline) {
            auto info = request_manager_->get_request(correlation_id);
            if (info && info->status != RpcStatus::PENDING && info->status != RpcStatus::IN_PROGRESS) {
                // Request completed
                auto* result = response->mutable_result();
                result->set_correlation_id(correlation_id);
                result->set_status(map_status(info->status));
                result->set_result_json(info->result_json);
                result->set_error_message(info->error_message);
                result->set_duration_ms(info->execution_time_ms);

                return grpc::Status::OK;
            }

            std::this_thread::sleep_for(poll_interval);
        }

        // Timeout
        auto* result = response->mutable_result();
        result->set_correlation_id(correlation_id);
        result->set_status(proto::RPC_TIMEOUT);
        result->set_error_message("Request timed out");

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "call_method failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::call_method_async(
    grpc::ServerContext* context,
    const proto::call_method_async_request* request,
    proto::call_method_async_response* response) {

    const auto& req = request->request();

    if (req.vehicle_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "vehicle_id is required");
    }
    if (req.service_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name is required");
    }
    if (req.method_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method_name is required");
    }

    int timeout_ms = req.timeout_ms();
    if (timeout_ms <= 0) {
        timeout_ms = 30000;
    }

    LOG(INFO) << "call_method_async: vehicle=" << req.vehicle_id()
              << " method=" << req.service_name() << "." << req.method_name();

    try {
        // Create request and track it
        std::string correlation_id = request_manager_->create_request(
            req.vehicle_id(),
            req.service_name(),
            req.method_name(),
            req.parameters_json(),
            req.requester_id(),
            std::chrono::milliseconds(timeout_ms));

        // Build and send the request payload
        std::string payload = build_request_payload(
            correlation_id,
            req.vehicle_id(),
            req.service_name(),
            req.method_name(),
            req.parameters_json(),
            timeout_ms);

        if (!producer_->send(req.vehicle_id(), payload)) {
            return grpc::Status(grpc::INTERNAL, "Failed to send request to Kafka");
        }

        // Return immediately with correlation_id
        auto* result = response->mutable_result();
        result->set_correlation_id(correlation_id);
        result->set_accepted(true);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "call_method_async failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::get_rpc_status(
    grpc::ServerContext* context,
    const proto::get_rpc_status_request* request,
    proto::get_rpc_status_response* response) {

    if (request->correlation_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "correlation_id is required");
    }

    try {
        auto info = request_manager_->get_request(request->correlation_id());
        if (!info) {
            return grpc::Status(grpc::NOT_FOUND, "Request not found");
        }

        auto* status = response->mutable_status();
        status->set_correlation_id(info->correlation_id);
        status->set_vehicle_id(info->vehicle_id);
        status->set_service_name(info->service_name);
        status->set_method_name(info->method_name);
        status->set_status(map_status(info->status));
        status->set_result_json(info->result_json);
        status->set_error_message(info->error_message);
        status->set_created_at_ms(info->created_at_ms);
        status->set_responded_at_ms(info->completed_at_ms);
        status->set_duration_ms(info->execution_time_ms);
        status->set_completed(info->status != RpcStatus::PENDING &&
                              info->status != RpcStatus::IN_PROGRESS);

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "get_rpc_status failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::list_rpc_requests(
    grpc::ServerContext* context,
    const proto::list_rpc_requests_request* request,
    proto::list_rpc_requests_response* response) {

    const auto& filter = request->filter();

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

    try {
        // Map filter status based on pending_only/failed_only flags
        RpcStatus filter_status = RpcStatus::PENDING;
        bool filter_by_status = false;
        if (filter.pending_only()) {
            filter_status = RpcStatus::PENDING;
            filter_by_status = true;
        } else if (filter.failed_only()) {
            filter_status = RpcStatus::ERROR;
            filter_by_status = true;
        }

        auto requests = request_manager_->list_requests(
            filter.vehicle_id_filter(),
            filter.service_name_filter(),
            filter_by_status ? filter_status : RpcStatus::PENDING,
            page_size,
            offset);

        auto* result = response->mutable_result();
        for (const auto& info : requests) {
            auto* req = result->add_requests();
            req->set_correlation_id(info.correlation_id);
            req->set_vehicle_id(info.vehicle_id);
            req->set_service_name(info.service_name);
            req->set_method_name(info.method_name);
            req->set_requester_id(info.requester_id);
            req->set_status(map_status(info.status));
            req->set_created_at_ms(info.created_at_ms);
            req->set_responded_at_ms(info.completed_at_ms);
        }

        if (static_cast<int>(requests.size()) == page_size) {
            result->set_next_page_token(std::to_string(offset + page_size));
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "list_rpc_requests failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::cancel_rpc(
    grpc::ServerContext* context,
    const proto::cancel_rpc_request* request,
    proto::cancel_rpc_response* response) {

    if (request->correlation_id().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "correlation_id is required");
    }

    try {
        bool cancelled = request_manager_->cancel_request(request->correlation_id());
        auto* result = response->mutable_result();
        result->set_success(cancelled);

        if (!cancelled) {
            result->set_error_message("Request not found or already completed");
        }

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "cancel_rpc failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::call_fleet_method(
    grpc::ServerContext* context,
    const proto::call_fleet_method_request* request,
    proto::call_fleet_method_response* response) {

    const auto& req = request->request();

    if (req.vehicle_ids().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "At least one vehicle_id is required");
    }
    if (req.service_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "service_name is required");
    }
    if (req.method_name().empty()) {
        return grpc::Status(grpc::INVALID_ARGUMENT, "method_name is required");
    }

    int timeout_ms = req.timeout_ms();
    if (timeout_ms <= 0) {
        timeout_ms = 30000;
    }

    LOG(INFO) << "call_fleet_method: " << req.vehicle_ids().size() << " vehicles"
              << " method=" << req.service_name() << "." << req.method_name();

    try {
        auto* result = response->mutable_result();

        // Send requests to all vehicles
        for (const auto& vehicle_id : req.vehicle_ids()) {
            std::string correlation_id = request_manager_->create_request(
                vehicle_id,
                req.service_name(),
                req.method_name(),
                req.parameters_json(),
                req.requester_id(),
                std::chrono::milliseconds(timeout_ms));

            std::string payload = build_request_payload(
                correlation_id,
                vehicle_id,
                req.service_name(),
                req.method_name(),
                req.parameters_json(),
                timeout_ms);

            auto* fleet_result = result->add_results();
            fleet_result->set_vehicle_id(vehicle_id);
            fleet_result->set_correlation_id(correlation_id);
            if (producer_->send(vehicle_id, payload)) {
                fleet_result->set_status(proto::RPC_PENDING);
            } else {
                fleet_result->set_status(proto::RPC_TRANSPORT_ERROR);
                fleet_result->set_error_message("Failed to send to Kafka");
            }
        }

        // Count successes and failures
        int successful = 0, failed = 0;
        for (const auto& r : result->results()) {
            if (r.status() == proto::RPC_PENDING) {
                successful++;
            } else {
                failed++;
            }
        }
        result->set_total_vehicles(result->results_size());
        result->set_successful(successful);
        result->set_failed(failed);
        result->set_pending(successful);  // All successful are pending

        return grpc::Status::OK;

    } catch (const std::exception& e) {
        LOG(ERROR) << "call_fleet_method failed: " << e.what();
        return grpc::Status(grpc::INTERNAL, e.what());
    }
}

grpc::Status CloudDispatcherServiceImpl::healthy(
    grpc::ServerContext* context,
    const proto::healthy_request* request,
    proto::healthy_response* response) {

    // Check if all dependencies are healthy
    bool healthy = is_healthy_ && request_manager_ && producer_;

    response->set_is_healthy(healthy);

    return grpc::Status::OK;
}

}  // namespace ifex::cloud::dispatcher
