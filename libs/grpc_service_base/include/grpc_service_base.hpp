#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

namespace ifex {
namespace cloud {

/**
 * Configuration for a gRPC cloud service
 */
struct GrpcServiceConfig {
    std::string listen_address = "0.0.0.0:50080";
    int num_threads = 4;
    bool enable_health_check = true;
    bool enable_reflection = true;
    int max_message_size_bytes = 64 * 1024 * 1024;  // 64MB
    int keepalive_time_ms = 10000;
    int keepalive_timeout_ms = 20000;
};

/**
 * Base class for cloud gRPC services
 *
 * Provides:
 * - Server lifecycle management (start/stop/graceful shutdown)
 * - Health check endpoint
 * - Signal handling (SIGINT/SIGTERM)
 * - Metrics collection (optional)
 */
class GrpcServiceBase {
public:
    explicit GrpcServiceBase(const GrpcServiceConfig& config);
    virtual ~GrpcServiceBase();

    // Non-copyable
    GrpcServiceBase(const GrpcServiceBase&) = delete;
    GrpcServiceBase& operator=(const GrpcServiceBase&) = delete;

    /**
     * Register a gRPC service implementation
     * Must be called before start()
     */
    void register_service(grpc::Service* service);

    /**
     * Start the server
     * Returns immediately, server runs in background
     */
    bool start();

    /**
     * Block until shutdown signal received
     */
    void wait();

    /**
     * Initiate graceful shutdown
     * @param deadline_ms Maximum time to wait for in-flight requests
     */
    void shutdown(int deadline_ms = 5000);

    /**
     * Check if server is running
     */
    bool is_running() const { return running_.load(); }

    /**
     * Get the actual bound address (useful when port=0)
     */
    std::string bound_address() const { return bound_address_; }

    /**
     * Set shutdown callback (called when shutdown initiated)
     */
    void set_shutdown_callback(std::function<void()> callback) {
        shutdown_callback_ = std::move(callback);
    }

protected:
    /**
     * Override to add custom initialization before server starts
     */
    virtual bool on_start() { return true; }

    /**
     * Override to add custom cleanup after server stops
     */
    virtual void on_stop() {}

private:
    void setup_signal_handlers();
    void build_and_start();

    GrpcServiceConfig config_;
    std::unique_ptr<grpc::Server> server_;
    std::vector<grpc::Service*> services_;
    std::string bound_address_;
    std::atomic<bool> running_{false};
    std::function<void()> shutdown_callback_;

    static std::atomic<GrpcServiceBase*> active_instance_;
    static void signal_handler(int signum);
};

/**
 * Helper to run a gRPC service with common setup
 *
 * Usage:
 *   return run_grpc_service<MyServiceImpl>(argc, argv, [](auto& builder) {
 *       // Configure service
 *   });
 */
template<typename ServiceImpl>
int run_grpc_service(
    int argc,
    char* argv[],
    const std::string& service_name,
    std::function<std::unique_ptr<ServiceImpl>(const GrpcServiceConfig&)> factory,
    const GrpcServiceConfig& config = GrpcServiceConfig())
{
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Starting " << service_name << " on " << config.listen_address;

    auto service_impl = factory(config);
    if (!service_impl) {
        LOG(ERROR) << "Failed to create service implementation";
        return 1;
    }

    GrpcServiceBase server(config);
    server.register_service(service_impl.get());

    if (!server.start()) {
        LOG(ERROR) << "Failed to start gRPC server";
        return 1;
    }

    LOG(INFO) << service_name << " listening on " << server.bound_address();
    server.wait();

    LOG(INFO) << service_name << " shutdown complete";
    return 0;
}

}  // namespace cloud
}  // namespace ifex
