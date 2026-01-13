/// @file backend_transport_server_main.cpp
/// @brief CloudBackendTransportServer main binary.
///
/// This server exposes the IFEX cloud_backend_transport_service gRPC interface,
/// using Kafka+MQTT as the underlying transport.
///
/// Usage:
///   ./backend_transport_server \
///     --grpc_listen=0.0.0.0:50100 \
///     --kafka_broker=localhost:9092 \
///     --content_id=202

#include "cloud_backend_transport_server.hpp"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>

#include <csignal>
#include <atomic>
#include <thread>

// gRPC flags
DEFINE_string(grpc_listen, "0.0.0.0:50100", "gRPC listen address");
DEFINE_int32(grpc_threads, 4, "Number of gRPC server threads");

// Kafka flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_group_id, "cloud-backend-transport", "Kafka consumer group ID");

// Content ID - determines which topic pattern to use
DEFINE_uint32(content_id, 202, "Content ID for topic routing");

// Topic names (optional - defaults based on content_id)
DEFINE_string(kafka_topic_v2c, "", "Kafka topic for v2c messages (default: ifex.scheduler.{content_id})");
DEFINE_string(kafka_topic_c2v, "", "Kafka topic for c2v messages (default: ifex.c2v.scheduler)");
DEFINE_string(kafka_topic_status, "ifex.status", "Kafka topic for vehicle status");

// Channel binding
DEFINE_uint32(partition_id, 0, "Partition ID for this instance");
DEFINE_uint32(total_partitions, 1, "Total number of partitions");

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

    LOG(INFO) << "CloudBackendTransportServer starting...";
    LOG(INFO) << "  gRPC listen: " << FLAGS_grpc_listen;
    LOG(INFO) << "  Kafka: " << FLAGS_kafka_broker;
    LOG(INFO) << "  Content ID: " << FLAGS_content_id;

    // Build configuration
    ifex::offboard::CloudBackendTransportServer::Config config;
    config.kafka_broker = FLAGS_kafka_broker;
    config.kafka_group_id = FLAGS_kafka_group_id;
    config.content_id = FLAGS_content_id;
    config.partition_id = FLAGS_partition_id;
    config.total_partitions = FLAGS_total_partitions;
    config.kafka_topic_status = FLAGS_kafka_topic_status;

    // Set topic names - use defaults if not specified
    if (FLAGS_kafka_topic_v2c.empty()) {
        config.kafka_topic_v2c = "ifex.scheduler." + std::to_string(FLAGS_content_id);
    } else {
        config.kafka_topic_v2c = FLAGS_kafka_topic_v2c;
    }

    if (FLAGS_kafka_topic_c2v.empty()) {
        config.kafka_topic_c2v = "ifex.c2v.scheduler";
    } else {
        config.kafka_topic_c2v = FLAGS_kafka_topic_c2v;
    }

    LOG(INFO) << "  Topic v2c: " << config.kafka_topic_v2c;
    LOG(INFO) << "  Topic c2v: " << config.kafka_topic_c2v;

    // Create transport server
    ifex::offboard::CloudBackendTransportServer server(config);

    if (!server.Start()) {
        LOG(ERROR) << "Failed to start transport server";
        return 1;
    }
    LOG(INFO) << "Transport started";

    // Create gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(FLAGS_grpc_listen, grpc::InsecureServerCredentials());

    // Register all service interfaces
    // The CloudBackendTransportServer implements multiple gRPC service interfaces
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::send_to_vehicle_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::get_vehicle_status_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::get_channel_info_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::get_queue_status_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::get_stats_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::healthy_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::on_vehicle_message_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::on_ack_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::on_vehicle_status_service::Service*>(&server));
    builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::on_queue_status_changed_service::Service*>(&server));

    std::unique_ptr<grpc::Server> grpc_server = builder.BuildAndStart();
    if (!grpc_server) {
        LOG(ERROR) << "Failed to start gRPC server";
        server.Stop();
        return 1;
    }

    LOG(INFO) << "CloudBackendTransportServer listening on " << FLAGS_grpc_listen;

    // Wait for shutdown signal
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "Shutting down...";
    grpc_server->Shutdown();
    server.Stop();

    LOG(INFO) << "CloudBackendTransportServer shutdown complete";
    return 0;
}
