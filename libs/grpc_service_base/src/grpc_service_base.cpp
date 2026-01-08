#include "grpc_service_base.hpp"

#include <csignal>
#include <chrono>
#include <thread>

#include <glog/logging.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

namespace ifex {
namespace cloud {

// Static member initialization
std::atomic<GrpcServiceBase*> GrpcServiceBase::active_instance_{nullptr};

void GrpcServiceBase::signal_handler(int signum) {
    // Don't call shutdown directly from signal handler - it causes deadlock
    // because we're on the same thread that's blocked in Wait()
    GrpcServiceBase* instance = active_instance_.load();
    if (instance && !instance->shutdown_requested_.exchange(true)) {
        // Spawn detached thread to perform shutdown
        std::thread([instance, signum]() {
            LOG(INFO) << "Received signal " << signum << ", initiating shutdown...";
            instance->shutdown();
        }).detach();
    }
}

GrpcServiceBase::GrpcServiceBase(const GrpcServiceConfig& config)
    : config_(config) {
}

GrpcServiceBase::~GrpcServiceBase() {
    if (running_.load()) {
        shutdown();
    }
}

void GrpcServiceBase::register_service(grpc::Service* service) {
    services_.push_back(service);
}

void GrpcServiceBase::setup_signal_handlers() {
    // Only one instance can handle signals at a time
    GrpcServiceBase* expected = nullptr;
    if (active_instance_.compare_exchange_strong(expected, this)) {
        signal(SIGINT, signal_handler);
        signal(SIGTERM, signal_handler);
    }
}

void GrpcServiceBase::build_and_start() {
    grpc::ServerBuilder builder;

    // Configure listen address
    int selected_port = 0;
    builder.AddListeningPort(config_.listen_address,
                            grpc::InsecureServerCredentials(),
                            &selected_port);

    // Configure message size limits
    builder.SetMaxReceiveMessageSize(config_.max_message_size_bytes);
    builder.SetMaxSendMessageSize(config_.max_message_size_bytes);

    // Configure keepalive
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS,
                               config_.keepalive_time_ms);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
                               config_.keepalive_timeout_ms);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);

    // Register services
    for (auto* service : services_) {
        builder.RegisterService(service);
    }

    // Enable health check
    if (config_.enable_health_check) {
        grpc::EnableDefaultHealthCheckService(true);
    }

    // Enable reflection for debugging
    if (config_.enable_reflection) {
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    }

    // Build and start server
    server_ = builder.BuildAndStart();

    if (server_) {
        // Extract actual bound address
        if (selected_port > 0) {
            // Replace port in address
            auto colon_pos = config_.listen_address.rfind(':');
            if (colon_pos != std::string::npos) {
                bound_address_ = config_.listen_address.substr(0, colon_pos + 1)
                               + std::to_string(selected_port);
            } else {
                bound_address_ = config_.listen_address + ":"
                               + std::to_string(selected_port);
            }
        } else {
            bound_address_ = config_.listen_address;
        }
        running_.store(true);
    }
}

bool GrpcServiceBase::start() {
    if (running_.load()) {
        LOG(WARNING) << "Server already running";
        return false;
    }

    if (services_.empty()) {
        LOG(ERROR) << "No services registered";
        return false;
    }

    // Custom initialization
    if (!on_start()) {
        LOG(ERROR) << "Custom initialization failed";
        return false;
    }

    // Setup signal handlers
    setup_signal_handlers();

    // Build and start server
    build_and_start();

    if (!server_) {
        LOG(ERROR) << "Failed to build gRPC server";
        return false;
    }

    return true;
}

void GrpcServiceBase::wait() {
    if (server_) {
        server_->Wait();
    }
}

void GrpcServiceBase::shutdown(int deadline_ms) {
    if (!running_.exchange(false)) {
        return;  // Already shutting down
    }

    LOG(INFO) << "Shutting down gRPC server...";

    // Call shutdown callback
    if (shutdown_callback_) {
        shutdown_callback_();
    }

    // Graceful shutdown with deadline
    if (server_) {
        auto deadline = std::chrono::system_clock::now()
                      + std::chrono::milliseconds(deadline_ms);
        server_->Shutdown(deadline);
    }

    // Custom cleanup
    on_stop();

    // Clear active instance
    GrpcServiceBase* expected = this;
    active_instance_.compare_exchange_strong(expected, nullptr);

    LOG(INFO) << "gRPC server shutdown complete";
}

}  // namespace cloud
}  // namespace ifex
