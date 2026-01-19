#include <gflags/gflags.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/health_check_service_interface.h>

#include <atomic>
#include <csignal>
#include <thread>

#include "postgres_client.hpp"
#include "scheduler_service_impl.hpp"
#include "job_sync_producer.hpp"

// gRPC flags
DEFINE_string(grpc_listen, "0.0.0.0:50083", "gRPC listen address");
DEFINE_int32(grpc_threads, 4, "Number of gRPC server threads");

// Kafka flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_topic_c2v, "ifex.c2v.scheduler", "Kafka topic for c2v scheduler commands");

// PostgreSQL flags
DEFINE_string(postgres_host, "localhost", "PostgreSQL host");
DEFINE_int32(postgres_port, 5432, "PostgreSQL port");
DEFINE_string(postgres_db, "ifex_offboard", "PostgreSQL database");
DEFINE_string(postgres_user, "ifex", "PostgreSQL user");
DEFINE_string(postgres_password, "ifex_dev", "PostgreSQL password");

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

    LOG(INFO) << "Cloud Scheduler Service (IFEX) starting...";
    LOG(INFO) << "  gRPC listen: " << FLAGS_grpc_listen;
    LOG(INFO) << "  Kafka: " << FLAGS_kafka_broker;
    LOG(INFO) << "  PostgreSQL: " << FLAGS_postgres_host << ":" << FLAGS_postgres_port
              << "/" << FLAGS_postgres_db;

    // Create PostgreSQL client
    ifex::offboard::PostgresConfig pg_config;
    pg_config.host = FLAGS_postgres_host;
    pg_config.port = FLAGS_postgres_port;
    pg_config.database = FLAGS_postgres_db;
    pg_config.user = FLAGS_postgres_user;
    pg_config.password = FLAGS_postgres_password;

    auto pg_client = std::make_shared<ifex::offboard::PostgresClient>(pg_config);
    if (!pg_client->is_connected()) {
        LOG(ERROR) << "Failed to connect to PostgreSQL";
        return 1;
    }
    LOG(INFO) << "Connected to PostgreSQL";

    // Create job sync producer (pure state sync model)
    ifex::cloud::scheduler::JobSyncProducerConfig producer_config;
    producer_config.brokers = FLAGS_kafka_broker;
    producer_config.topic = FLAGS_kafka_topic_c2v;
    producer_config.client_id = "cloud-scheduler-producer";

    auto producer = std::make_shared<ifex::cloud::scheduler::JobSyncProducer>(producer_config);
    if (!producer->init()) {
        LOG(ERROR) << "Failed to initialize job sync producer";
        return 1;
    }
    LOG(INFO) << "Job sync producer initialized";

    // Create service implementation (inherits from all IFEX service classes)
    auto service_impl = std::make_unique<ifex::cloud::scheduler::CloudSchedulerServiceImpl>(
        pg_client, producer);

    // Enable gRPC health checking and reflection
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();

    // Create gRPC server
    grpc::ServerBuilder builder;
    builder.AddListeningPort(FLAGS_grpc_listen, grpc::InsecureServerCredentials());

    // Register all IFEX service interfaces (one per method)
    // The service_impl inherits from all these service base classes
    namespace proto = swdv::cloud_scheduler_service;
    builder.RegisterService(static_cast<proto::create_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::update_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::delete_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::pause_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::resume_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::trigger_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::get_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::list_jobs_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::get_job_executions_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::create_fleet_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::delete_fleet_job_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::get_fleet_job_stats_service::Service*>(service_impl.get()));
    builder.RegisterService(static_cast<proto::healthy_service::Service*>(service_impl.get()));

    std::unique_ptr<grpc::Server> grpc_server = builder.BuildAndStart();
    if (!grpc_server) {
        LOG(ERROR) << "Failed to start gRPC server";
        return 1;
    }

    LOG(INFO) << "Cloud Scheduler Service listening on " << FLAGS_grpc_listen;

    // Wait for shutdown signal
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "Shutting down...";

    // Shutdown gRPC server
    grpc_server->Shutdown();

    // Flush producer
    producer->flush(5000);

    // Print final stats
    const auto& stats = producer->stats();
    LOG(INFO) << "Final stats:";
    LOG(INFO) << "  sync_messages_sent=" << stats.sync_messages_sent;
    LOG(INFO) << "  trigger_requests_sent=" << stats.trigger_requests_sent;
    LOG(INFO) << "  send_errors=" << stats.send_errors;

    LOG(INFO) << "Cloud Scheduler Service shutdown complete";
    return 0;
}
