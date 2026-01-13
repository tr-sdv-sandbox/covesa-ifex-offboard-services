#include <gflags/gflags.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>

#include <atomic>
#include <csignal>
#include <thread>

#include "dispatcher_service_impl.hpp"
#include "rpc_request_manager.hpp"
#include "request_producer.hpp"
#include "response_consumer.hpp"

// gRPC flags
DEFINE_string(grpc_listen, "0.0.0.0:50082", "gRPC listen address");
DEFINE_int32(grpc_threads, 4, "Number of gRPC server threads");

// Kafka flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_topic_c2v, "ifex.c2v.rpc", "Kafka topic for c2v RPC requests");
DEFINE_string(kafka_topic_v2c, "ifex.rpc.200", "Kafka topic for v2c RPC responses");
DEFINE_string(kafka_group, "cloud-dispatcher", "Kafka consumer group ID");

// RPC defaults
DEFINE_int32(default_timeout_ms, 30000, "Default RPC timeout in milliseconds");

static std::atomic<bool> g_shutdown{false};

void signal_handler(int sig) {
    LOG(INFO) << "Received signal " << sig << ", shutting down...";
    g_shutdown = true;
}

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Install signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    LOG(INFO) << "Cloud Dispatcher Service (IFEX) starting...";
    LOG(INFO) << "  gRPC listen: " << FLAGS_grpc_listen;
    LOG(INFO) << "  Kafka: " << FLAGS_kafka_broker;
    LOG(INFO) << "  c2v topic: " << FLAGS_kafka_topic_c2v;
    LOG(INFO) << "  v2c topic: " << FLAGS_kafka_topic_v2c;

    // Create RPC request manager (in-memory tracking)
    auto request_manager = std::make_shared<ifex::cloud::dispatcher::RpcRequestManager>();

    // Create request producer (for c2v)
    ifex::cloud::dispatcher::RequestProducerConfig producer_config;
    producer_config.brokers = FLAGS_kafka_broker;
    producer_config.topic = FLAGS_kafka_topic_c2v;
    producer_config.client_id = "cloud-dispatcher-producer";

    auto producer = std::make_shared<ifex::cloud::dispatcher::RequestProducer>(producer_config);
    if (!producer->init()) {
        LOG(ERROR) << "Failed to initialize request producer";
        return 1;
    }
    LOG(INFO) << "Request producer initialized";

    // Create and start response consumer (for v2c)
    ifex::cloud::dispatcher::ResponseConsumerConfig consumer_config;
    consumer_config.brokers = FLAGS_kafka_broker;
    consumer_config.topic = FLAGS_kafka_topic_v2c;
    consumer_config.group_id = FLAGS_kafka_group;
    consumer_config.client_id = "cloud-dispatcher-consumer";

    ifex::cloud::dispatcher::ResponseConsumer response_consumer(
        consumer_config, request_manager);
    response_consumer.start();
    LOG(INFO) << "Response consumer started";

    // Create service implementation (inherits from all IFEX service classes)
    auto service_impl = std::make_unique<ifex::cloud::dispatcher::CloudDispatcherServiceImpl>(
        request_manager, producer);

    // Enable gRPC health checking and reflection
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    // Create gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(FLAGS_grpc_listen, grpc::InsecureServerCredentials());

    // Register all IFEX service interfaces (one per method)
    // The service_impl inherits from all these service base classes
    namespace proto = swdv::cloud_dispatcher_service;
    builder.RegisterService(static_cast<proto::call_method_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::call_method_async_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::get_rpc_status_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::list_rpc_requests_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::cancel_rpc_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::call_fleet_method_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::healthy_service::Service*>(service_impl.get()));

    std::unique_ptr<grpc::Server> grpc_server = builder.BuildAndStart();
    if (!grpc_server) {
        LOG(ERROR) << "Failed to start gRPC server";
        response_consumer.stop();
        return 1;
    }

    LOG(INFO) << "Cloud Dispatcher Service listening on " << FLAGS_grpc_listen;

    // Wait for shutdown signal
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "Shutting down...";

    // Shutdown gRPC server
    grpc_server->Shutdown();

    // Stop consumer and flush producer
    response_consumer.stop();
    producer->flush(5000);

    // Print final stats
    const auto& consumer_stats = response_consumer.stats();
    const auto& producer_stats = producer->stats();
    LOG(INFO) << "Final stats:";
    LOG(INFO) << "  requests_sent=" << producer_stats.messages_sent;
    LOG(INFO) << "  send_errors=" << producer_stats.send_errors;
    LOG(INFO) << "  responses_received=" << consumer_stats.messages_received;
    LOG(INFO) << "  responses_processed=" << consumer_stats.responses_processed;
    LOG(INFO) << "  parse_errors=" << consumer_stats.parse_errors;

    LOG(INFO) << "Cloud Dispatcher Service shutdown complete";
    return 0;
}
