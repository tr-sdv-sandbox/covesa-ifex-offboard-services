/**
 * Dispatcher Service E2E Tests
 *
 * Tests the Cloud Dispatcher Service gRPC API including successful RPC round-trips.
 * The dispatcher_api service provides RPC dispatch to vehicles via Kafka → MQTT.
 *
 * RPC tracking is in-memory only (no database persistence).
 *
 * Prerequisites:
 *   - PostgreSQL running (for test vehicle setup)
 *   - Kafka running (for RPC message flow)
 *   - dispatcher_api service started by test fixture
 *
 * Run: ./tests/dispatcher_e2e_test
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <linux/limits.h>
#include <memory>
#include <random>
#include <signal.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

#include <grpcpp/grpcpp.h>
#include <librdkafka/rdkafkacpp.h>

#include "e2e_test_fixture.hpp"
#include "postgres_client.hpp"
#include "cloud-dispatcher-service.grpc.pb.h"
#include "rpc_offboard_codec.hpp"
#include "dispatcher-rpc-envelope.pb.h"

// IFEX-based types namespace
namespace proto = swdv::cloud_dispatcher_service;

/// Get directory containing the current executable
std::string GetExecutableDir() {
    char path[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (len == -1) return ".";
    path[len] = '\0';
    std::string str(path);
    size_t pos = str.rfind('/');
    return (pos != std::string::npos) ? str.substr(0, pos) : ".";
}

/// Get path to a service binary (in build root, one level up from tests/)
std::string GetBinaryPath(const std::string& name) {
    return GetExecutableDir() + "/../" + name;
}

// Test configuration - uses E2E isolated infrastructure
// Ports offset from standard to avoid conflicts with simulation runs
static const char* DISPATCHER_API_HOST = "localhost";
static const int DISPATCHER_API_PORT = 50082;

// E2E isolated infrastructure (see tests/e2e/docker-compose.e2e.yml)
static const char* KAFKA_BROKER = "localhost:19092";     // Standard: 9092
static const char* KAFKA_TOPIC_V2C = "e2e.rpc.200";      // E2E-prefixed topic
static const char* KAFKA_TOPIC_C2V = "e2e.c2v.rpc";      // E2E-prefixed topic
static const int POSTGRES_PORT = 15432;                   // Standard: 5432

static const char* VEHICLE_ID = "RPC_TEST_VEHICLE_001";
static const char* VEHICLE_ID_2 = "RPC_TEST_VEHICLE_002";
static const char* FLEET_ID = "test-fleet-rpc";
static const char* REGION = "eu-west-1";

class DispatcherE2ETest : public ::testing::Test {
protected:
    // IFEX-based stubs (one per method)
    static std::unique_ptr<proto::call_method_service::Stub> call_method_stub_;
    static std::unique_ptr<proto::call_method_async_service::Stub> call_method_async_stub_;
    static std::unique_ptr<proto::call_fleet_method_service::Stub> call_fleet_method_stub_;
    static std::unique_ptr<proto::get_rpc_status_service::Stub> get_rpc_status_stub_;
    static std::unique_ptr<proto::cancel_rpc_service::Stub> cancel_rpc_stub_;
    static std::unique_ptr<proto::list_rpc_requests_service::Stub> list_rpc_requests_stub_;
    static std::unique_ptr<proto::healthy_service::Stub> healthy_stub_;
    static std::shared_ptr<grpc::Channel> channel_;
    static pid_t dispatcher_api_pid_;
    static std::unique_ptr<ifex::offboard::PostgresClient> db_;
    static std::unique_ptr<RdKafka::Producer> kafka_producer_;
    static bool kafka_available_;

    static void EnsureKafkaTopicExists(const std::string& topic) {
        // Auto-create topic by producing a dummy message
        // Kafka (configured with auto.create.topics.enable=true) will create the topic
        if (!kafka_producer_) return;

        const char* dummy = "";
        RdKafka::ErrorCode err = kafka_producer_->produce(
            topic,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY,
            const_cast<char*>(dummy), 0,
            nullptr, 0,
            0, nullptr);

        if (err != RdKafka::ERR_NO_ERROR) {
            LOG(WARNING) << "Failed to auto-create topic " << topic << ": " << RdKafka::err2str(err);
        } else {
            kafka_producer_->poll(0);
            kafka_producer_->flush(1000);
            LOG(INFO) << "Ensured Kafka topic exists: " << topic;
        }

        // Give Kafka time to create the topic
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    static void SetUpTestSuite() {
        // Connect to PostgreSQL first (needed for test vehicle setup)
        // Uses E2E isolated port to avoid conflicts with simulation
        ifex::offboard::PostgresConfig pg_config;
        pg_config.host = "localhost";
        pg_config.port = POSTGRES_PORT;  // E2E port: 15432
        pg_config.database = "ifex_offboard";
        pg_config.user = "ifex";
        pg_config.password = "ifex_dev";

        db_ = std::make_unique<ifex::offboard::PostgresClient>(pg_config);
        ASSERT_TRUE(db_->is_connected()) << "Failed to connect to PostgreSQL at port " << POSTGRES_PORT
            << ". Run: docker compose -f tests/e2e/docker-compose.e2e.yml up -d";

        // Create test vehicles
        auto result = db_->execute(R"(
            INSERT INTO vehicles (vehicle_id, is_online, first_seen_at, last_seen_at)
            VALUES ($1, true, NOW(), NOW())
            ON CONFLICT (vehicle_id) DO UPDATE SET is_online = true, last_seen_at = NOW()
        )", {VEHICLE_ID});
        ASSERT_TRUE(result.ok()) << "Failed to create test vehicle 1: " << result.error();

        result = db_->execute(R"(
            INSERT INTO vehicles (vehicle_id, is_online, first_seen_at, last_seen_at)
            VALUES ($1, false, NOW(), NOW())
            ON CONFLICT (vehicle_id) DO UPDATE SET is_online = false, last_seen_at = NOW()
        )", {VEHICLE_ID_2});
        ASSERT_TRUE(result.ok()) << "Failed to create test vehicle 2: " << result.error();

        LOG(INFO) << "Created test vehicles: " << VEHICLE_ID << ", " << VEHICLE_ID_2;

        // Initialize Kafka producer for simulating vehicle responses
        std::string errstr;
        auto conf = std::unique_ptr<RdKafka::Conf>(
            RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
        conf->set("bootstrap.servers", KAFKA_BROKER, errstr);
        conf->set("client.id", "dispatcher-e2e-test", errstr);

        kafka_producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
        if (kafka_producer_) {
            kafka_available_ = true;
            LOG(INFO) << "Kafka producer initialized";

            // Ensure the Kafka topic exists (auto-create)
            EnsureKafkaTopicExists(KAFKA_TOPIC_V2C);
        } else {
            kafka_available_ = false;
            LOG(WARNING) << "Kafka not available: " << errstr << " - skipping round-trip tests";
        }

        // Start dispatcher_api service with unique consumer group to avoid stale offsets
        std::string binary_path = GetBinaryPath("dispatcher_api");
        std::string grpc_listen = std::string(DISPATCHER_API_HOST) + ":" + std::to_string(DISPATCHER_API_PORT);
        std::string kafka_group = "dispatcher-e2e-test-" + std::to_string(getpid());

        dispatcher_api_pid_ = fork();
        if (dispatcher_api_pid_ == 0) {
            // Child process - exec dispatcher_api with E2E topics
            execl(binary_path.c_str(), "dispatcher_api",
                  "--grpc_listen", grpc_listen.c_str(),
                  "--kafka_broker", KAFKA_BROKER,
                  "--kafka_topic_v2c", KAFKA_TOPIC_V2C,
                  "--kafka_topic_c2v", KAFKA_TOPIC_C2V,
                  "--kafka_group", kafka_group.c_str(),
                  "--logtostderr",
                  nullptr);
            LOG(ERROR) << "Failed to exec dispatcher_api at: " << binary_path;
            _exit(1);
        }

        ASSERT_GT(dispatcher_api_pid_, 0) << "Failed to fork dispatcher_api process";
        LOG(INFO) << "Started dispatcher_api (E2E) with PID " << dispatcher_api_pid_
                  << " (Kafka=" << KAFKA_BROKER << ", topics=" << KAFKA_TOPIC_V2C << "/" << KAFKA_TOPIC_C2V << ")";

        // Wait for service to be ready (gRPC startup)
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // Warm-up: Send a dummy request and response to ensure consumer is ready
        // New consumer groups take time to join and get partition assignments
        if (kafka_available_) {
            LOG(INFO) << "Warming up Kafka consumer...";

            // Create a dummy RPC to warm up the Kafka consumer
            swdv::dispatcher_rpc_envelope::rpc_response_t warmup_response;
            warmup_response.set_correlation_id("warmup-test");
            warmup_response.set_status(swdv::dispatcher_rpc_envelope::SUCCESS);
            warmup_response.set_result_json("{}");

            std::string payload = ifex::offboard::encode_rpc_response_offboard(
                "warmup-vehicle", warmup_response);

            kafka_producer_->produce(
                std::string(KAFKA_TOPIC_V2C),
                RdKafka::Topic::PARTITION_UA,
                RdKafka::Producer::RK_MSG_COPY,
                const_cast<char*>(payload.data()),
                payload.size(),
                nullptr, 0, 0, nullptr);
            kafka_producer_->flush(1000);

            // Wait for consumer to process (indicates it's ready)
            std::this_thread::sleep_for(std::chrono::seconds(3));
            LOG(INFO) << "Kafka warm-up complete";
        }

        // Connect gRPC client
        std::string target = std::string(DISPATCHER_API_HOST) + ":" + std::to_string(DISPATCHER_API_PORT);
        channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());

        // Create all IFEX stubs (one per method)
        call_method_stub_ = proto::call_method_service::NewStub(channel_);
        call_method_async_stub_ = proto::call_method_async_service::NewStub(channel_);
        call_fleet_method_stub_ = proto::call_fleet_method_service::NewStub(channel_);
        get_rpc_status_stub_ = proto::get_rpc_status_service::NewStub(channel_);
        cancel_rpc_stub_ = proto::cancel_rpc_service::NewStub(channel_);
        list_rpc_requests_stub_ = proto::list_rpc_requests_service::NewStub(channel_);
        healthy_stub_ = proto::healthy_service::NewStub(channel_);

        // Wait for connection
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        if (!channel_->WaitForConnected(deadline)) {
            FAIL() << "Failed to connect to dispatcher_api at " << target;
        }

        LOG(INFO) << "dispatcher_api is ready";
    }

    static void TearDownTestSuite() {
        // Close gRPC connection first to avoid shutdown delay
        call_method_stub_.reset();
        call_method_async_stub_.reset();
        call_fleet_method_stub_.reset();
        get_rpc_status_stub_.reset();
        cancel_rpc_stub_.reset();
        list_rpc_requests_stub_.reset();
        healthy_stub_.reset();
        channel_.reset();

        // Stop dispatcher_api
        if (dispatcher_api_pid_ > 0) {
            LOG(INFO) << "Stopping dispatcher_api (PID " << dispatcher_api_pid_ << ")";
            kill(dispatcher_api_pid_, SIGTERM);
            int status;
            waitpid(dispatcher_api_pid_, &status, 0);
        }

        // Flush and destroy Kafka producer
        if (kafka_producer_) {
            kafka_producer_->flush(1000);
            kafka_producer_.reset();
        }

        // Clean up test data
        if (db_ && db_->is_connected()) {
            db_->execute("DELETE FROM vehicle_enrichment WHERE vehicle_id LIKE 'RPC_TEST_%'", {});
            db_->execute("DELETE FROM vehicles WHERE vehicle_id LIKE 'RPC_TEST_%'", {});
            LOG(INFO) << "Cleaned up test data";
        }
    }

    void SetUp() override {
        ASSERT_TRUE(call_method_stub_ != nullptr) << "gRPC stubs not initialized";
    }
};

// Initialize static members
std::unique_ptr<proto::call_method_service::Stub> DispatcherE2ETest::call_method_stub_;
std::unique_ptr<proto::call_method_async_service::Stub> DispatcherE2ETest::call_method_async_stub_;
std::unique_ptr<proto::call_fleet_method_service::Stub> DispatcherE2ETest::call_fleet_method_stub_;
std::unique_ptr<proto::get_rpc_status_service::Stub> DispatcherE2ETest::get_rpc_status_stub_;
std::unique_ptr<proto::cancel_rpc_service::Stub> DispatcherE2ETest::cancel_rpc_stub_;
std::unique_ptr<proto::list_rpc_requests_service::Stub> DispatcherE2ETest::list_rpc_requests_stub_;
std::unique_ptr<proto::healthy_service::Stub> DispatcherE2ETest::healthy_stub_;
std::shared_ptr<grpc::Channel> DispatcherE2ETest::channel_;
pid_t DispatcherE2ETest::dispatcher_api_pid_ = 0;
std::unique_ptr<ifex::offboard::PostgresClient> DispatcherE2ETest::db_;
std::unique_ptr<RdKafka::Producer> DispatcherE2ETest::kafka_producer_;
bool DispatcherE2ETest::kafka_available_ = false;

// =============================================================================
// API Validation Tests (no vehicle needed - just test input validation)
// =============================================================================

TEST_F(DispatcherE2ETest, CallMethodAsync_ValidationEmptyVehicleId) {
    grpc::ClientContext context;
    proto::call_method_async_request request;
    proto::call_method_async_response response;

    auto* inner = request.mutable_request();
    inner->set_service_name("TestService");
    inner->set_method_name("TestMethod");

    auto status = call_method_async_stub_->call_method_async(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.error_message().find("vehicle_id") != std::string::npos);
}

TEST_F(DispatcherE2ETest, CallMethodAsync_ValidationEmptyServiceName) {
    grpc::ClientContext context;
    proto::call_method_async_request request;
    proto::call_method_async_response response;

    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_method_name("TestMethod");

    auto status = call_method_async_stub_->call_method_async(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.error_message().find("service_name") != std::string::npos);
}

TEST_F(DispatcherE2ETest, CallMethodAsync_ValidationEmptyMethodName) {
    grpc::ClientContext context;
    proto::call_method_async_request request;
    proto::call_method_async_response response;

    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_service_name("TestService");

    auto status = call_method_async_stub_->call_method_async(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.error_message().find("method_name") != std::string::npos);
}

// =============================================================================
// CallMethod Validation Tests
// =============================================================================

TEST_F(DispatcherE2ETest, CallMethod_ValidationEmptyVehicleId) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

    proto::call_method_request request;
    proto::call_method_response response;

    auto* inner = request.mutable_request();
    inner->set_service_name("TestService");
    inner->set_method_name("TestMethod");

    auto status = call_method_stub_->call_method(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(DispatcherE2ETest, CallMethod_ValidationEmptyServiceName) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

    proto::call_method_request request;
    proto::call_method_response response;

    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_method_name("TestMethod");

    auto status = call_method_stub_->call_method(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(DispatcherE2ETest, CallMethod_ValidationEmptyMethodName) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));

    proto::call_method_request request;
    proto::call_method_response response;

    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_service_name("TestService");

    auto status = call_method_stub_->call_method(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

// =============================================================================
// CallFleetMethod Validation Tests
// =============================================================================

TEST_F(DispatcherE2ETest, CallFleetMethod_ValidationEmptyVehicleIds) {
    grpc::ClientContext context;
    proto::call_fleet_method_request request;
    proto::call_fleet_method_response response;

    auto* inner = request.mutable_request();
    inner->set_service_name("TestService");
    inner->set_method_name("TestMethod");

    auto status = call_fleet_method_stub_->call_fleet_method(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
    EXPECT_TRUE(status.error_message().find("vehicle") != std::string::npos);
}

TEST_F(DispatcherE2ETest, CallFleetMethod_ValidationEmptyServiceName) {
    grpc::ClientContext context;
    proto::call_fleet_method_request request;
    proto::call_fleet_method_response response;

    auto* inner = request.mutable_request();
    inner->add_vehicle_ids(VEHICLE_ID);
    inner->set_method_name("TestMethod");

    auto status = call_fleet_method_stub_->call_fleet_method(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(DispatcherE2ETest, CallFleetMethod_ValidationEmptyMethodName) {
    grpc::ClientContext context;
    proto::call_fleet_method_request request;
    proto::call_fleet_method_response response;

    auto* inner = request.mutable_request();
    inner->add_vehicle_ids(VEHICLE_ID);
    inner->set_service_name("TestService");

    auto status = call_fleet_method_stub_->call_fleet_method(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

// =============================================================================
// GetRpcStatus Validation Tests
// =============================================================================

TEST_F(DispatcherE2ETest, GetRpcStatus_EmptyCorrelationId) {
    grpc::ClientContext context;
    proto::get_rpc_status_request request;
    proto::get_rpc_status_response response;

    auto status = get_rpc_status_stub_->get_rpc_status(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(DispatcherE2ETest, GetRpcStatus_NotFound) {
    grpc::ClientContext context;
    proto::get_rpc_status_request request;
    proto::get_rpc_status_response response;

    request.set_correlation_id("nonexistent-correlation-id-12345");

    auto status = get_rpc_status_stub_->get_rpc_status(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// =============================================================================
// CancelRpc Validation Tests
// =============================================================================

TEST_F(DispatcherE2ETest, CancelRpc_EmptyCorrelationId) {
    grpc::ClientContext context;
    proto::cancel_rpc_request request;
    proto::cancel_rpc_response response;

    auto status = cancel_rpc_stub_->cancel_rpc(&context, request, &response);
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

TEST_F(DispatcherE2ETest, CancelRpc_NotFound) {
    grpc::ClientContext context;
    proto::cancel_rpc_request request;
    proto::cancel_rpc_response response;

    request.set_correlation_id("nonexistent-cancel-id-12345");

    auto status = cancel_rpc_stub_->cancel_rpc(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "CancelRpc failed: " << status.error_message();
    EXPECT_FALSE(response.result().success());
}

// =============================================================================
// ListRpcRequests Validation Tests
// =============================================================================

TEST_F(DispatcherE2ETest, ListRpcRequests_EmptyResults) {
    grpc::ClientContext context;
    proto::list_rpc_requests_request request;
    proto::list_rpc_requests_response response;

    request.mutable_filter()->set_page_size(10);

    auto status = list_rpc_requests_stub_->list_rpc_requests(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "ListRpcRequests failed: " << status.error_message();
}

// =============================================================================
// Real Vehicle Tests (requires ifex-vehicle:latest Docker image)
// =============================================================================

// Note: e2e_test_fixture.hpp is included at top of file

/**
 * @brief E2E tests with real vehicle container running echo service
 *
 * These tests verify the complete cloud→vehicle→cloud round-trip:
 *   Cloud API → Kafka → mqtt_kafka_bridge → MQTT → Vehicle → MQTT → Kafka → Cloud
 *
 * Prerequisites:
 *   - Docker image: ifex-vehicle:latest (build with covesa-ifex-core/build-test-container.sh)
 *   - Infrastructure: postgres, kafka, mosquitto (run deploy/start-infra.sh)
 */
class DispatcherRealVehicleTest : public ifex::offboard::test::E2ETestFixture {
protected:
    static constexpr const char* DISPATCHER_ADDR = "localhost:50082";
    // IFEX-based stubs
    static std::unique_ptr<proto::call_method_service::Stub> call_method_stub_;
    static std::unique_ptr<proto::call_method_async_service::Stub> call_method_async_stub_;
    static std::unique_ptr<proto::get_rpc_status_service::Stub> get_rpc_status_stub_;
    static std::shared_ptr<grpc::Channel> channel_;
    static pid_t dispatcher_pid_;

    static void SetUpTestSuite() {
        // Start E2E infrastructure (vehicle, bridges)
        ifex::offboard::test::E2ETestFixture::SetUpTestSuite();

        // Wait for echo_service to register
        ASSERT_TRUE(ifex::offboard::test::E2ETestInfrastructure::WaitForServiceRegistered(
            ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID,
            "echo_service",
            std::chrono::seconds(30)))
            << "Echo service did not register within 30 seconds";

        // Start dispatcher_api
        std::string binary = ifex::offboard::test::E2ETestInfrastructure::GetBinaryPath("dispatcher_api");

        dispatcher_pid_ = fork();
        if (dispatcher_pid_ == 0) {
            // Use E2E Kafka infrastructure
            execl(binary.c_str(), "dispatcher_api",
                  "--grpc_listen", DISPATCHER_ADDR,
                  "--kafka_broker", ifex::offboard::test::E2ETestInfrastructure::KAFKA_BROKER,
                  "--kafka_topic_v2c", "e2e.rpc.200",
                  "--kafka_topic_c2v", "e2e.c2v.rpc",
                  "--logtostderr",
                  nullptr);
            LOG(ERROR) << "Failed to exec dispatcher_api";
            _exit(1);
        }
        ASSERT_GT(dispatcher_pid_, 0) << "Failed to fork dispatcher_api";
        LOG(INFO) << "Started dispatcher_api (E2E) with PID " << dispatcher_pid_;

        // Connect gRPC client
        std::this_thread::sleep_for(std::chrono::seconds(2));
        channel_ = grpc::CreateChannel(DISPATCHER_ADDR, grpc::InsecureChannelCredentials());
        call_method_stub_ = proto::call_method_service::NewStub(channel_);
        call_method_async_stub_ = proto::call_method_async_service::NewStub(channel_);
        get_rpc_status_stub_ = proto::get_rpc_status_service::NewStub(channel_);

        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
        ASSERT_TRUE(channel_->WaitForConnected(deadline))
            << "Failed to connect to dispatcher_api";

        // Warm-up: Send a test RPC to ensure Kafka consumer is fully initialized
        // The first RPC after dispatcher_api starts can be flaky due to consumer startup
        LOG(INFO) << "Warming up dispatcher with test RPC...";
        {
            grpc::ClientContext warmup_ctx;
            warmup_ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

            proto::call_method_request warmup_req;
            auto* inner = warmup_req.mutable_request();
            inner->set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
            inner->set_service_name("echo_service");
            inner->set_method_name("echo");
            inner->set_parameters_json(R"({"message": "warmup"})");
            inner->set_timeout_ms(15000);

            proto::call_method_response warmup_resp;
            auto warmup_status = call_method_stub_->call_method(&warmup_ctx, warmup_req, &warmup_resp);
            if (warmup_status.ok() && warmup_resp.result().status() == proto::RPC_SUCCESS) {
                LOG(INFO) << "Warm-up RPC succeeded, dispatcher ready";
            } else {
                LOG(WARNING) << "Warm-up RPC failed (expected on first run): "
                             << warmup_status.error_message();
                // Give more time for consumer to initialize
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
        }

        LOG(INFO) << "Real vehicle test infrastructure ready";
    }

    static void TearDownTestSuite() {
        // Close gRPC first
        call_method_stub_.reset();
        call_method_async_stub_.reset();
        get_rpc_status_stub_.reset();
        channel_.reset();

        // Stop dispatcher_api
        if (dispatcher_pid_ > 0) {
            LOG(INFO) << "Stopping dispatcher_api (PID " << dispatcher_pid_ << ")";
            kill(dispatcher_pid_, SIGTERM);
            int status;
            waitpid(dispatcher_pid_, &status, 0);
            dispatcher_pid_ = 0;
        }

        // Stop E2E infrastructure
        ifex::offboard::test::E2ETestFixture::TearDownTestSuite();
    }
};

// Static members
std::unique_ptr<proto::call_method_service::Stub> DispatcherRealVehicleTest::call_method_stub_;
std::unique_ptr<proto::call_method_async_service::Stub> DispatcherRealVehicleTest::call_method_async_stub_;
std::unique_ptr<proto::get_rpc_status_service::Stub> DispatcherRealVehicleTest::get_rpc_status_stub_;
std::shared_ptr<grpc::Channel> DispatcherRealVehicleTest::channel_;
pid_t DispatcherRealVehicleTest::dispatcher_pid_ = 0;

TEST_F(DispatcherRealVehicleTest, RealVehicle_EchoRoundTrip) {
    // NOTE: Test is DISABLED until echo service registration is verified working
    // Enable once vehicle infrastructure is confirmed

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

    proto::call_method_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
    inner->set_service_name("echo_service");
    inner->set_method_name("echo");
    inner->set_parameters_json(R"({"message": "Hello from real E2E test!"})");
    inner->set_timeout_ms(15000);

    proto::call_method_response response;
    auto status = call_method_stub_->call_method(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "CallMethod failed: " << status.error_message();
    EXPECT_EQ(response.result().status(), proto::RPC_SUCCESS);
    // JSON may or may not have spaces after colons depending on serializer
    EXPECT_TRUE(response.result().result_json() == R"({"response":"Hello from real E2E test!"})" ||
                response.result().result_json() == R"({"response": "Hello from real E2E test!"})")
        << "Actual: " << response.result().result_json();
}

TEST_F(DispatcherRealVehicleTest, RealVehicle_EchoWithDelay) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

    proto::call_method_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
    inner->set_service_name("echo_service");
    inner->set_method_name("echo_with_delay");
    inner->set_parameters_json(R"({"message": "Delayed message", "delay_ms": 500})");
    inner->set_timeout_ms(15000);

    auto start = std::chrono::steady_clock::now();
    proto::call_method_response response;
    auto status = call_method_stub_->call_method(&context, request, &response);
    auto elapsed = std::chrono::steady_clock::now() - start;

    ASSERT_TRUE(status.ok()) << "CallMethod failed: " << status.error_message();
    EXPECT_EQ(response.result().status(), proto::RPC_SUCCESS);

    // Should have taken at least 500ms
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
    EXPECT_GE(elapsed_ms, 500) << "Response came back too quickly";

    LOG(INFO) << "echo_with_delay took " << elapsed_ms << "ms";
}

TEST_F(DispatcherRealVehicleTest, RealVehicle_ConcatStrings) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));

    proto::call_method_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
    inner->set_service_name("echo_service");
    inner->set_method_name("concat");
    inner->set_parameters_json(R"({"first": "Hello", "second": "World", "separator": ", "})");
    inner->set_timeout_ms(15000);

    proto::call_method_response response;
    auto status = call_method_stub_->call_method(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "CallMethod failed: " << status.error_message();
    EXPECT_EQ(response.result().status(), proto::RPC_SUCCESS);
    // JSON may or may not have spaces after colons depending on serializer
    EXPECT_TRUE(response.result().result_json() == R"({"result":"Hello, World"})" ||
                response.result().result_json() == R"({"result": "Hello, World"})")
        << "Actual: " << response.result().result_json();
}

TEST_F(DispatcherRealVehicleTest, RealVehicle_AsyncRoundTrip) {
    // Step 1: Send async RPC
    grpc::ClientContext async_ctx;
    proto::call_method_async_request async_req;
    auto* inner = async_req.mutable_request();
    inner->set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
    inner->set_service_name("echo_service");
    inner->set_method_name("echo");
    inner->set_parameters_json(R"({"message": "Async test"})");
    inner->set_timeout_ms(30000);

    proto::call_method_async_response async_resp;
    auto status = call_method_async_stub_->call_method_async(&async_ctx, async_req, &async_resp);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(async_resp.result().accepted());

    std::string corr_id = async_resp.result().correlation_id();
    LOG(INFO) << "Async request sent, correlation_id=" << corr_id;

    // Step 2: Poll for completion (real vehicle response)
    proto::cloud_rpc_status_t final_status = proto::RPC_PENDING;
    std::string result;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);

    while (std::chrono::steady_clock::now() < deadline) {
        grpc::ClientContext poll_ctx;
        proto::get_rpc_status_request poll_req;
        poll_req.set_correlation_id(corr_id);

        proto::get_rpc_status_response poll_resp;
        auto s = get_rpc_status_stub_->get_rpc_status(&poll_ctx, poll_req, &poll_resp);
        ASSERT_TRUE(s.ok());

        final_status = poll_resp.status().status();
        result = poll_resp.status().result_json();

        if (final_status != proto::RPC_PENDING) {
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    EXPECT_EQ(final_status, proto::RPC_SUCCESS);
    // JSON may or may not have spaces after colons depending on serializer
    EXPECT_TRUE(result == R"({"response":"Async test"})" ||
                result == R"({"response": "Async test"})")
        << "Actual: " << result;
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
