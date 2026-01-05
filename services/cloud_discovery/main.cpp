#include <gflags/gflags.h>
#include <glog/logging.h>

#include "grpc_service_base.hpp"
#include "postgres_client.hpp"
#include "discovery_service_impl.hpp"

// gRPC flags
DEFINE_string(grpc_listen, "0.0.0.0:50081", "gRPC listen address");
DEFINE_int32(grpc_threads, 4, "Number of gRPC server threads");

// PostgreSQL flags
DEFINE_string(postgres_host, "localhost", "PostgreSQL host");
DEFINE_int32(postgres_port, 5432, "PostgreSQL port");
DEFINE_string(postgres_db, "ifex_offboard", "PostgreSQL database");
DEFINE_string(postgres_user, "ifex", "PostgreSQL user");
DEFINE_string(postgres_password, "ifex_dev", "PostgreSQL password");

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "Cloud Discovery Service starting...";
    LOG(INFO) << "  gRPC listen: " << FLAGS_grpc_listen;
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

    // Create gRPC service config
    ifex::cloud::GrpcServiceConfig grpc_config;
    grpc_config.listen_address = FLAGS_grpc_listen;
    grpc_config.num_threads = FLAGS_grpc_threads;
    grpc_config.enable_health_check = true;
    grpc_config.enable_reflection = true;

    // Create service implementation
    auto service_impl = std::make_unique<ifex::cloud::discovery::CloudDiscoveryServiceImpl>(
        pg_client);

    // Create and start gRPC server
    ifex::cloud::GrpcServiceBase server(grpc_config);
    server.register_service(service_impl.get());

    if (!server.start()) {
        LOG(ERROR) << "Failed to start gRPC server";
        return 1;
    }

    LOG(INFO) << "Cloud Discovery Service listening on " << server.bound_address();
    server.wait();

    LOG(INFO) << "Cloud Discovery Service shutdown complete";
    return 0;
}
