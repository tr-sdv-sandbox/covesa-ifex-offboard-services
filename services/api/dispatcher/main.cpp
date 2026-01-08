#include <gflags/gflags.h>
#include <glog/logging.h>

#include "grpc_service_base.hpp"
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

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Cloud Dispatcher Service starting...";
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

    // Create gRPC service config
    ifex::cloud::GrpcServiceConfig grpc_config;
    grpc_config.listen_address = FLAGS_grpc_listen;
    grpc_config.num_threads = FLAGS_grpc_threads;
    grpc_config.enable_health_check = true;
    grpc_config.enable_reflection = true;

    // Create service implementation
    auto service_impl = std::make_unique<ifex::cloud::dispatcher::CloudDispatcherServiceImpl>(
        request_manager, producer);

    // Create and start gRPC server
    ifex::cloud::GrpcServiceBase server(grpc_config);
    server.register_service(service_impl.get());

    // Set shutdown callback to stop consumer
    server.set_shutdown_callback([&response_consumer, &producer]() {
        response_consumer.stop();
        producer->flush(5000);
    });

    if (!server.start()) {
        LOG(ERROR) << "Failed to start gRPC server";
        return 1;
    }

    LOG(INFO) << "Cloud Dispatcher Service listening on " << server.bound_address();
    server.wait();

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
