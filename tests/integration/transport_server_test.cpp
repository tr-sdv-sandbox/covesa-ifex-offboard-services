/**
 * @file transport_server_test.cpp
 * @brief Integration tests for CloudBackendTransportServer
 *
 * Tests the gRPC interface of the Kafka+MQTT based transport server.
 * Uses E2E Docker infrastructure (Kafka, MQTT, PostgreSQL).
 *
 * Run: docker compose -f tests/e2e/docker-compose.e2e.yml up -d
 * Then: ctest -R transport_server_test
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <grpcpp/grpcpp.h>

#include "cloud_backend_transport_server.hpp"
#include "cloud-backend-transport-service.grpc.pb.h"

#include <thread>
#include <chrono>

namespace ifex::offboard::test {

// E2E infrastructure ports (from docker-compose.e2e.yml)
static constexpr const char* E2E_KAFKA_BROKER = "localhost:19092";
static constexpr const char* E2E_MQTT_HOST = "localhost";
static constexpr int E2E_MQTT_PORT = 11883;

class TransportServerTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        // Verify E2E Kafka is running
        LOG(INFO) << "Checking E2E infrastructure...";

        int ret = system("docker ps --format '{{.Names}}' | grep -q '^e2e-kafka$'");
        if (ret != 0) {
            LOG(WARNING) << "E2E Kafka not running. Starting Docker infrastructure...";
            ret = system("docker compose -f tests/e2e/docker-compose.e2e.yml up -d 2>&1 || "
                         "docker-compose -f tests/e2e/docker-compose.e2e.yml up -d 2>&1");
            if (ret != 0) {
                // Try from build directory
                ret = system("docker compose -f ../tests/e2e/docker-compose.e2e.yml up -d 2>&1 || "
                             "docker-compose -f ../tests/e2e/docker-compose.e2e.yml up -d 2>&1");
            }
            // Give Kafka time to start
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }

        // Create test topics
        LOG(INFO) << "Creating test Kafka topics...";
        system("docker exec e2e-kafka /opt/kafka/bin/kafka-topics.sh "
               "--bootstrap-server localhost:29092 --create --if-not-exists "
               "--topic transport.test.v2c --partitions 1 --replication-factor 1 >/dev/null 2>&1");
        system("docker exec e2e-kafka /opt/kafka/bin/kafka-topics.sh "
               "--bootstrap-server localhost:29092 --create --if-not-exists "
               "--topic transport.test.c2v --partitions 1 --replication-factor 1 >/dev/null 2>&1");
        system("docker exec e2e-kafka /opt/kafka/bin/kafka-topics.sh "
               "--bootstrap-server localhost:29092 --create --if-not-exists "
               "--topic transport.test.status --partitions 1 --replication-factor 1 >/dev/null 2>&1");

        LOG(INFO) << "E2E infrastructure ready";
    }

    void SetUp() override {
        // Configure server with E2E infrastructure
        CloudBackendTransportServer::Config config;
        config.kafka_broker = E2E_KAFKA_BROKER;
        config.kafka_group_id = "transport-test-" + std::to_string(getpid()) + "-" +
                                 std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        config.kafka_topic_v2c = "transport.test.v2c";
        config.kafka_topic_c2v = "transport.test.c2v";
        config.kafka_topic_status = "transport.test.status";
        config.content_id = 999;  // Test content ID
        config.partition_id = 0;
        config.total_partitions = 1;

        server_ = std::make_unique<CloudBackendTransportServer>(config);

        // Start the transport
        ASSERT_TRUE(server_->Start()) << "Failed to start transport server";

        // Build gRPC server
        grpc::ServerBuilder builder;
        int selected_port = 0;
        builder.AddListeningPort("localhost:0", grpc::InsecureServerCredentials(), &selected_port);

        // Register services
        builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::send_to_vehicle_service::Service*>(server_.get()));
        builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::get_channel_info_service::Service*>(server_.get()));
        builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::healthy_service::Service*>(server_.get()));
        builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::get_stats_service::Service*>(server_.get()));
        builder.RegisterService(static_cast<swdv::cloud_backend_transport_service::get_vehicle_status_service::Service*>(server_.get()));

        grpc_server_ = builder.BuildAndStart();
        ASSERT_TRUE(grpc_server_) << "Failed to start gRPC server";

        server_address_ = "localhost:" + std::to_string(selected_port);
        LOG(INFO) << "Test server started on " << server_address_;

        // Create client channel
        channel_ = grpc::CreateChannel(server_address_, grpc::InsecureChannelCredentials());
    }

    void TearDown() override {
        if (grpc_server_) {
            grpc_server_->Shutdown();
        }
        if (server_) {
            server_->Stop();
        }
    }

    std::unique_ptr<CloudBackendTransportServer> server_;
    std::unique_ptr<grpc::Server> grpc_server_;
    std::string server_address_;
    std::shared_ptr<grpc::Channel> channel_;
};

TEST_F(TransportServerTest, HealthyReturnsTrue) {
    auto stub = swdv::cloud_backend_transport_service::healthy_service::NewStub(channel_);

    grpc::ClientContext context;
    swdv::cloud_backend_transport_service::healthy_request request;
    swdv::cloud_backend_transport_service::healthy_response response;

    auto status = stub->healthy(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.is_healthy());
}

TEST_F(TransportServerTest, GetChannelInfoReturnsConfig) {
    auto stub = swdv::cloud_backend_transport_service::get_channel_info_service::NewStub(channel_);

    grpc::ClientContext context;
    swdv::cloud_backend_transport_service::get_channel_info_request request;
    swdv::cloud_backend_transport_service::get_channel_info_response response;

    auto status = stub->get_channel_info(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_EQ(response.info().content_id(), 999);
    EXPECT_EQ(response.info().partition_id(), 0);
    EXPECT_EQ(response.info().total_partitions(), 1);
}

TEST_F(TransportServerTest, GetStatsInitiallyZero) {
    auto stub = swdv::cloud_backend_transport_service::get_stats_service::NewStub(channel_);

    grpc::ClientContext context;
    swdv::cloud_backend_transport_service::get_stats_request request;
    swdv::cloud_backend_transport_service::get_stats_response response;

    auto status = stub->get_stats(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    // Initially, no messages have been sent/received
    EXPECT_EQ(response.stats().messages_sent(), 0);
    EXPECT_EQ(response.stats().messages_received(), 0);
}

TEST_F(TransportServerTest, SendToVehicleReturnsSequence) {
    auto stub = swdv::cloud_backend_transport_service::send_to_vehicle_service::NewStub(channel_);

    grpc::ClientContext context;
    swdv::cloud_backend_transport_service::send_to_vehicle_request request;
    swdv::cloud_backend_transport_service::send_to_vehicle_response response;

    auto* req = request.mutable_request();
    req->set_vehicle_id("TEST_VIN_001");
    req->set_payload("Hello, vehicle!");
    req->set_persistence(swdv::cloud_backend_transport_service::persistence_t::VOLATILE);

    auto status = stub->send_to_vehicle(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    // Should get a sequence number back
    EXPECT_GT(response.result().sequence(), 0UL);
    EXPECT_EQ(response.result().status(), swdv::cloud_backend_transport_service::publish_status_t::OK);
}

TEST_F(TransportServerTest, SendToVehicleIncrementsSentCount) {
    auto send_stub = swdv::cloud_backend_transport_service::send_to_vehicle_service::NewStub(channel_);
    auto stats_stub = swdv::cloud_backend_transport_service::get_stats_service::NewStub(channel_);

    // Send a message
    {
        grpc::ClientContext context;
        swdv::cloud_backend_transport_service::send_to_vehicle_request request;
        swdv::cloud_backend_transport_service::send_to_vehicle_response response;

        auto* req = request.mutable_request();
        req->set_vehicle_id("TEST_VIN_002");
        req->set_payload("Test message for stats");
        req->set_persistence(swdv::cloud_backend_transport_service::persistence_t::BEST_EFFORT);

        auto status = send_stub->send_to_vehicle(&context, request, &response);
        ASSERT_TRUE(status.ok());
    }

    // Check stats
    {
        grpc::ClientContext context;
        swdv::cloud_backend_transport_service::get_stats_request request;
        swdv::cloud_backend_transport_service::get_stats_response response;

        auto status = stats_stub->get_stats(&context, request, &response);
        ASSERT_TRUE(status.ok());
        EXPECT_GE(response.stats().messages_sent(), 1);
    }
}

TEST_F(TransportServerTest, GetVehicleStatusReturnsUnknownForNewVehicle) {
    auto stub = swdv::cloud_backend_transport_service::get_vehicle_status_service::NewStub(channel_);

    grpc::ClientContext context;
    swdv::cloud_backend_transport_service::get_vehicle_status_request request;
    swdv::cloud_backend_transport_service::get_vehicle_status_response response;

    request.set_vehicle_id("UNKNOWN_VEHICLE_XYZ");

    auto status = stub->get_vehicle_status(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_EQ(response.status(), swdv::cloud_backend_transport_service::vehicle_status_t::UNKNOWN);
}

}  // namespace ifex::offboard::test

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
