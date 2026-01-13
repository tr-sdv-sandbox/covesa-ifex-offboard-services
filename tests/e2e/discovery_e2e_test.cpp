/**
 * Discovery Service E2E Tests
 *
 * Tests the Cloud Discovery Service gRPC API against real PostgreSQL database.
 * The discovery_api service provides fleet-wide service registry queries.
 *
 * Prerequisites:
 *   - PostgreSQL running (docker compose up -d postgres)
 *   - discovery_api service started by test fixture
 *
 * Run: ./tests/discovery_e2e_test
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

#include "postgres_client.hpp"
#include "e2e_test_fixture.hpp"
#include "cloud-discovery-service.grpc.pb.h"

// IFEX-based types namespace
namespace proto = swdv::cloud_discovery_service;
using ifex::offboard::test::E2ETestInfrastructure;

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

// Test configuration
static const char* DISCOVERY_API_HOST = "localhost";
static const int DISCOVERY_API_PORT = 50081;
static const char* VEHICLE_ID = "DISC_TEST_VEHICLE_001";
static const char* VEHICLE_ID_2 = "DISC_TEST_VEHICLE_002";
static const char* FLEET_ID = "test-fleet-discovery";
static const char* REGION = "eu-west-1";

class DiscoveryE2ETest : public ::testing::Test {
protected:
    // IFEX-based stubs (one per method)
    static std::unique_ptr<proto::list_vehicles_service::Stub> list_vehicles_stub_;
    static std::unique_ptr<proto::get_vehicle_services_service::Stub> get_vehicle_services_stub_;
    static std::unique_ptr<proto::query_services_by_name_service::Stub> query_services_by_name_stub_;
    static std::unique_ptr<proto::get_fleet_service_stats_service::Stub> get_fleet_service_stats_stub_;
    static std::unique_ptr<proto::find_vehicles_with_service_service::Stub> find_vehicles_with_service_stub_;
    static std::unique_ptr<proto::healthy_service::Stub> healthy_stub_;
    static std::shared_ptr<grpc::Channel> channel_;
    static pid_t discovery_api_pid_;
    static std::unique_ptr<ifex::offboard::PostgresClient> db_;

    // Generate unique test IDs
    static std::string generate_id(const std::string& prefix) {
        static std::random_device rd;
        static std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        std::stringstream ss;
        ss << prefix << "-" << std::hex << dis(gen);
        return ss.str();
    }

    static void SetUpTestSuite() {
        // Connect to PostgreSQL using E2E isolated ports
        ifex::offboard::PostgresConfig pg_config;
        pg_config.host = "localhost";
        pg_config.port = E2ETestInfrastructure::POSTGRES_PORT;  // E2E port: 15432
        pg_config.database = "ifex_offboard";
        pg_config.user = "ifex";
        pg_config.password = "ifex_dev";

        db_ = std::make_unique<ifex::offboard::PostgresClient>(pg_config);
        ASSERT_TRUE(db_->is_connected()) << "Failed to connect to PostgreSQL on E2E port";

        // Reset database schema for clean test state
        LOG(INFO) << "Resetting database schema...";
        E2ETestInfrastructure::ResetDatabase(db_.get());

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

        // Create vehicle enrichment
        result = db_->execute(R"(
            INSERT INTO vehicle_enrichment (vehicle_id, fleet_id, region, model, year)
            VALUES ($1, $2, $3, 'Test Model', 2025)
            ON CONFLICT (vehicle_id) DO UPDATE SET fleet_id = $2, region = $3
        )", {VEHICLE_ID, FLEET_ID, REGION});
        ASSERT_TRUE(result.ok()) << "Failed to create vehicle enrichment 1: " << result.error();

        result = db_->execute(R"(
            INSERT INTO vehicle_enrichment (vehicle_id, fleet_id, region, model, year)
            VALUES ($1, $2, $3, 'Test Model 2', 2024)
            ON CONFLICT (vehicle_id) DO UPDATE SET fleet_id = $2, region = $3
        )", {VEHICLE_ID_2, FLEET_ID, "us-east-1"});
        ASSERT_TRUE(result.ok()) << "Failed to create vehicle enrichment 2: " << result.error();

        LOG(INFO) << "Created test vehicles: " << VEHICLE_ID << ", " << VEHICLE_ID_2;

        // Start discovery_api service with E2E isolated ports
        std::string binary_path = GetBinaryPath("discovery_api");
        std::string grpc_listen = std::string(DISCOVERY_API_HOST) + ":" + std::to_string(DISCOVERY_API_PORT);
        std::string pg_port = std::to_string(E2ETestInfrastructure::POSTGRES_PORT);

        discovery_api_pid_ = fork();
        if (discovery_api_pid_ == 0) {
            // Child process - exec discovery_api
            execl(binary_path.c_str(), "discovery_api",
                  "--grpc_listen", grpc_listen.c_str(),
                  "--postgres_host", "localhost",
                  "--postgres_port", pg_port.c_str(),
                  "--postgres_db", "ifex_offboard",
                  "--postgres_user", "ifex",
                  "--postgres_password", "ifex_dev",
                  nullptr);
            // If exec fails, exit child
            LOG(ERROR) << "Failed to exec discovery_api at: " << binary_path;
            _exit(1);
        }

        ASSERT_GT(discovery_api_pid_, 0) << "Failed to fork discovery_api process";
        LOG(INFO) << "Started discovery_api with PID " << discovery_api_pid_;

        // Wait for service to be ready
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Connect gRPC client
        std::string target = std::string(DISCOVERY_API_HOST) + ":" + std::to_string(DISCOVERY_API_PORT);
        channel_ = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());

        // Create all IFEX stubs (one per method)
        list_vehicles_stub_ = proto::list_vehicles_service::NewStub(channel_);
        get_vehicle_services_stub_ = proto::get_vehicle_services_service::NewStub(channel_);
        query_services_by_name_stub_ = proto::query_services_by_name_service::NewStub(channel_);
        get_fleet_service_stats_stub_ = proto::get_fleet_service_stats_service::NewStub(channel_);
        find_vehicles_with_service_stub_ = proto::find_vehicles_with_service_service::NewStub(channel_);
        healthy_stub_ = proto::healthy_service::NewStub(channel_);

        // Wait for connection
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(5);
        if (!channel_->WaitForConnected(deadline)) {
            FAIL() << "Failed to connect to discovery_api at " << target;
        }

        LOG(INFO) << "discovery_api is ready";
    }

    static void TearDownTestSuite() {
        // Stop discovery_api
        if (discovery_api_pid_ > 0) {
            LOG(INFO) << "Stopping discovery_api (PID " << discovery_api_pid_ << ")";
            kill(discovery_api_pid_, SIGTERM);
            int status;
            waitpid(discovery_api_pid_, &status, 0);
        }

        // Clean up test data
        if (db_ && db_->is_connected()) {
            // Clean up vehicle_schemas first (FK constraint)
            db_->execute("DELETE FROM vehicle_schemas WHERE vehicle_id LIKE 'DISC_TEST_%'", {});

            // Clean up enrichment (FK constraint)
            db_->execute("DELETE FROM vehicle_enrichment WHERE vehicle_id LIKE 'DISC_TEST_%'", {});

            // Clean up vehicles
            db_->execute("DELETE FROM vehicles WHERE vehicle_id LIKE 'DISC_TEST_%'", {});

            LOG(INFO) << "Cleaned up test vehicles";
        }
    }

    void SetUp() override {
        ASSERT_TRUE(list_vehicles_stub_ != nullptr) << "gRPC stubs not initialized";
    }

    // Helper to create a test service in the database using new schema tables
    void CreateTestService(const std::string& vehicle_id,
                           const std::string& service_name,
                           const std::string& version,
                           const std::string& status = "available") {
        // Generate a unique schema hash for this test service
        std::string schema_hash = generate_id("hash") + "0000000000000000000000000000000000000000000000";
        schema_hash = schema_hash.substr(0, 64);  // SHA-256 is 64 hex chars

        // Insert into schema_registry
        auto result = db_->execute(R"(
            INSERT INTO schema_registry (schema_hash, ifex_schema, service_name, version,
                                         methods, struct_definitions, enum_definitions)
            VALUES ($1, $2, $3, $4, '[]'::jsonb, '{}'::jsonb, '{}'::jsonb)
            ON CONFLICT (schema_hash) DO NOTHING
        )", {schema_hash, "# Test IFEX schema for " + service_name, service_name, version});
        ASSERT_TRUE(result.ok()) << "Failed to create schema_registry entry: " << result.error();

        // Link vehicle to schema
        result = db_->execute(R"(
            INSERT INTO vehicle_schemas (vehicle_id, schema_hash, last_seen_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (vehicle_id, schema_hash) DO UPDATE SET last_seen_at = NOW()
        )", {vehicle_id, schema_hash});
        ASSERT_TRUE(result.ok()) << "Failed to create vehicle_schemas entry: " << result.error();
    }

    // Helper to clean up test services
    void CleanupTestServices() {
        db_->execute("DELETE FROM vehicle_schemas WHERE vehicle_id LIKE 'DISC_TEST_%'", {});
        db_->execute("DELETE FROM schema_registry WHERE service_name LIKE '%_service'", {});
    }
};

// Static member initialization
std::unique_ptr<proto::list_vehicles_service::Stub> DiscoveryE2ETest::list_vehicles_stub_;
std::unique_ptr<proto::get_vehicle_services_service::Stub> DiscoveryE2ETest::get_vehicle_services_stub_;
std::unique_ptr<proto::query_services_by_name_service::Stub> DiscoveryE2ETest::query_services_by_name_stub_;
std::unique_ptr<proto::get_fleet_service_stats_service::Stub> DiscoveryE2ETest::get_fleet_service_stats_stub_;
std::unique_ptr<proto::find_vehicles_with_service_service::Stub> DiscoveryE2ETest::find_vehicles_with_service_stub_;
std::unique_ptr<proto::healthy_service::Stub> DiscoveryE2ETest::healthy_stub_;
std::shared_ptr<grpc::Channel> DiscoveryE2ETest::channel_;
pid_t DiscoveryE2ETest::discovery_api_pid_ = 0;
std::unique_ptr<ifex::offboard::PostgresClient> DiscoveryE2ETest::db_;

// =============================================================================
// Connection Tests
// =============================================================================

TEST_F(DiscoveryE2ETest, CloudDiscoveryConnection) {
    // Verify the gRPC channel is connected
    ASSERT_NE(channel_, nullptr);
    auto state = channel_->GetState(false);
    EXPECT_EQ(state, GRPC_CHANNEL_READY);
    LOG(INFO) << "Cloud Discovery is available at " << DISCOVERY_API_HOST << ":" << DISCOVERY_API_PORT;
}

// =============================================================================
// ListVehicles Tests
// =============================================================================

TEST_F(DiscoveryE2ETest, ListVehiclesBasic) {
    grpc::ClientContext context;
    proto::list_vehicles_request request;
    proto::list_vehicles_response response;

    request.mutable_filter()->set_page_size(100);

    auto status = list_vehicles_stub_->list_vehicles(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "ListVehicles failed: " << status.error_message();
    EXPECT_GE(response.result().total_count(), 2) << "Expected at least 2 test vehicles";
    EXPECT_GE(response.result().vehicles_size(), 2);

    LOG(INFO) << "ListVehicles: total=" << response.result().total_count()
              << " returned=" << response.result().vehicles_size();

    // Verify our test vehicles are present
    bool found_v1 = false, found_v2 = false;
    for (const auto& v : response.result().vehicles()) {
        if (v.vehicle_id() == VEHICLE_ID) {
            found_v1 = true;
            EXPECT_TRUE(v.is_online());
            EXPECT_EQ(v.fleet_id(), FLEET_ID);
            EXPECT_EQ(v.region(), REGION);
        }
        if (v.vehicle_id() == VEHICLE_ID_2) {
            found_v2 = true;
            EXPECT_FALSE(v.is_online());
        }
    }
    EXPECT_TRUE(found_v1) << "Test vehicle 1 not found";
    EXPECT_TRUE(found_v2) << "Test vehicle 2 not found";
}

TEST_F(DiscoveryE2ETest, ListVehiclesOnlineOnly) {
    grpc::ClientContext context;
    proto::list_vehicles_request request;
    proto::list_vehicles_response response;

    auto* filter = request.mutable_filter();
    filter->set_online_only(true);
    filter->set_page_size(100);

    auto status = list_vehicles_stub_->list_vehicles(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "ListVehicles (online_only) failed: " << status.error_message();

    LOG(INFO) << "ListVehicles (online_only): total=" << response.result().total_count();

    // All returned vehicles should be online
    for (const auto& v : response.result().vehicles()) {
        EXPECT_TRUE(v.is_online()) << "Vehicle " << v.vehicle_id() << " should be online";
    }

    // VEHICLE_ID should be in the list, VEHICLE_ID_2 should not
    bool found_v1 = false, found_v2 = false;
    for (const auto& v : response.result().vehicles()) {
        if (v.vehicle_id() == VEHICLE_ID) found_v1 = true;
        if (v.vehicle_id() == VEHICLE_ID_2) found_v2 = true;
    }
    EXPECT_TRUE(found_v1) << "Online vehicle not found";
    EXPECT_FALSE(found_v2) << "Offline vehicle should not be in online_only list";
}

TEST_F(DiscoveryE2ETest, ListVehiclesWithFleetFilter) {
    grpc::ClientContext context;
    proto::list_vehicles_request request;
    proto::list_vehicles_response response;

    auto* filter = request.mutable_filter();
    filter->set_fleet_id_filter(FLEET_ID);
    filter->set_page_size(100);

    auto status = list_vehicles_stub_->list_vehicles(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "ListVehicles (fleet filter) failed: " << status.error_message();

    LOG(INFO) << "ListVehicles (fleet=" << FLEET_ID << "): total=" << response.result().total_count();

    // All returned vehicles should belong to our test fleet
    for (const auto& v : response.result().vehicles()) {
        EXPECT_EQ(v.fleet_id(), FLEET_ID) << "Vehicle " << v.vehicle_id()
            << " has unexpected fleet_id: " << v.fleet_id();
    }
}

TEST_F(DiscoveryE2ETest, ListVehiclesWithRegionFilter) {
    grpc::ClientContext context;
    proto::list_vehicles_request request;
    proto::list_vehicles_response response;

    auto* filter = request.mutable_filter();
    filter->set_region_filter(REGION);
    filter->set_page_size(100);

    auto status = list_vehicles_stub_->list_vehicles(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "ListVehicles (region filter) failed: " << status.error_message();

    LOG(INFO) << "ListVehicles (region=" << REGION << "): total=" << response.result().total_count();

    // All returned vehicles should be in our test region
    for (const auto& v : response.result().vehicles()) {
        EXPECT_EQ(v.region(), REGION) << "Vehicle " << v.vehicle_id()
            << " has unexpected region: " << v.region();
    }
}

TEST_F(DiscoveryE2ETest, ListVehiclesPagination) {
    grpc::ClientContext context1;
    proto::list_vehicles_request request;
    proto::list_vehicles_response response1;

    request.mutable_filter()->set_page_size(1);

    auto status = list_vehicles_stub_->list_vehicles(&context1, request, &response1);

    ASSERT_TRUE(status.ok()) << "ListVehicles (page 1) failed: " << status.error_message();
    EXPECT_EQ(response1.result().vehicles_size(), 1);
    EXPECT_GE(response1.result().total_count(), 2);

    if (response1.result().total_count() > 1) {
        EXPECT_FALSE(response1.result().next_page_token().empty()) << "Expected pagination token";

        // Get second page
        grpc::ClientContext context2;
        request.mutable_filter()->set_page_token(response1.result().next_page_token());
        proto::list_vehicles_response response2;

        status = list_vehicles_stub_->list_vehicles(&context2, request, &response2);

        ASSERT_TRUE(status.ok()) << "ListVehicles (page 2) failed: " << status.error_message();
        EXPECT_EQ(response2.result().vehicles_size(), 1);

        // Verify different vehicles
        EXPECT_NE(response1.result().vehicles(0).vehicle_id(), response2.result().vehicles(0).vehicle_id())
            << "Pagination returned same vehicle twice";

        LOG(INFO) << "Pagination: page1=" << response1.result().vehicles(0).vehicle_id()
                  << " page2=" << response2.result().vehicles(0).vehicle_id();
    }
}

// =============================================================================
// GetVehicleServices Tests
// =============================================================================

TEST_F(DiscoveryE2ETest, GetVehicleServicesEmpty) {
    grpc::ClientContext context;
    proto::get_vehicle_services_request request;
    proto::get_vehicle_services_response response;

    request.set_vehicle_id(VEHICLE_ID);

    auto status = get_vehicle_services_stub_->get_vehicle_services(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "GetVehicleServices failed: " << status.error_message();
    EXPECT_EQ(response.result().services_size(), 0) << "Expected no services for clean test vehicle";

    LOG(INFO) << "GetVehicleServices (empty): vehicle=" << VEHICLE_ID
              << " services=" << response.result().services_size();
}

TEST_F(DiscoveryE2ETest, GetVehicleServicesWithData) {
    // Create test services
    CreateTestService(VEHICLE_ID, "climate_service", "1.0.0", "available");
    CreateTestService(VEHICLE_ID, "navigation_service", "2.0.0", "available");

    grpc::ClientContext context;
    proto::get_vehicle_services_request request;
    proto::get_vehicle_services_response response;

    request.set_vehicle_id(VEHICLE_ID);

    auto status = get_vehicle_services_stub_->get_vehicle_services(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "GetVehicleServices failed: " << status.error_message();
    EXPECT_EQ(response.result().services_size(), 2) << "Expected 2 services";

    LOG(INFO) << "GetVehicleServices: vehicle=" << VEHICLE_ID
              << " services=" << response.result().services_size();

    for (const auto& svc : response.result().services()) {
        LOG(INFO) << "  - " << svc.service_name() << " v" << svc.version();
    }

    CleanupTestServices();
}

TEST_F(DiscoveryE2ETest, GetVehicleServicesMissingVehicleId) {
    grpc::ClientContext context;
    proto::get_vehicle_services_request request;
    proto::get_vehicle_services_response response;

    // Don't set vehicle_id

    auto status = get_vehicle_services_stub_->get_vehicle_services(&context, request, &response);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT);
}

// =============================================================================
// QueryServicesByName Tests
// =============================================================================

TEST_F(DiscoveryE2ETest, QueryServicesByNameExact) {
    // Create test services
    CreateTestService(VEHICLE_ID, "hvac_service", "1.0.0");
    CreateTestService(VEHICLE_ID_2, "hvac_service", "1.1.0");

    grpc::ClientContext context;
    proto::query_services_by_name_request request;
    proto::query_services_by_name_response response;

    auto* filter = request.mutable_filter();
    filter->set_service_name_pattern("hvac_service");
    filter->set_page_size(100);

    auto status = query_services_by_name_stub_->query_services_by_name(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "QueryServicesByName failed: " << status.error_message();
    EXPECT_GE(response.result().total_count(), 2) << "Expected at least 2 hvac_service instances";

    LOG(INFO) << "QueryServicesByName (hvac_service): total=" << response.result().total_count();

    for (const auto& loc : response.result().locations()) {
        EXPECT_EQ(loc.service().service_name(), "hvac_service");
        LOG(INFO) << "  - vehicle=" << loc.vehicle_id() << " version=" << loc.service().version();
    }

    CleanupTestServices();
}

TEST_F(DiscoveryE2ETest, QueryServicesByNamePattern) {
    // Create test services
    CreateTestService(VEHICLE_ID, "climate_hvac_service", "1.0.0");
    CreateTestService(VEHICLE_ID, "climate_ac_service", "1.0.0");
    CreateTestService(VEHICLE_ID, "navigation_service", "1.0.0");

    grpc::ClientContext context;
    proto::query_services_by_name_request request;
    proto::query_services_by_name_response response;

    auto* filter = request.mutable_filter();
    filter->set_service_name_pattern("climate%");
    filter->set_page_size(100);

    auto status = query_services_by_name_stub_->query_services_by_name(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "QueryServicesByName (pattern) failed: " << status.error_message();
    EXPECT_GE(response.result().total_count(), 2) << "Expected at least 2 climate* services";

    LOG(INFO) << "QueryServicesByName (climate%): total=" << response.result().total_count();

    // All returned services should match the pattern
    for (const auto& loc : response.result().locations()) {
        EXPECT_TRUE(loc.service().service_name().find("climate") == 0)
            << "Service " << loc.service().service_name() << " doesn't match pattern";
    }

    CleanupTestServices();
}

TEST_F(DiscoveryE2ETest, QueryServicesByNameMissingPattern) {
    grpc::ClientContext context;
    proto::query_services_by_name_request request;
    proto::query_services_by_name_response response;

    // Don't set service_name_pattern

    auto status = query_services_by_name_stub_->query_services_by_name(&context, request, &response);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT);
}

// =============================================================================
// GetFleetServiceStats Tests
// =============================================================================

TEST_F(DiscoveryE2ETest, GetFleetServiceStatsEmpty) {
    grpc::ClientContext context;
    proto::get_fleet_service_stats_request request;
    proto::get_fleet_service_stats_response response;

    // Use a non-existent fleet
    request.mutable_filter()->set_fleet_id_filter("nonexistent-fleet-xyz");

    auto status = get_fleet_service_stats_stub_->get_fleet_service_stats(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "GetFleetServiceStats failed: " << status.error_message();
    EXPECT_EQ(response.result().total_vehicles(), 0);
    EXPECT_EQ(response.result().total_services(), 0);

    LOG(INFO) << "GetFleetServiceStats (empty): total_vehicles=" << response.result().total_vehicles();
}

TEST_F(DiscoveryE2ETest, GetFleetServiceStatsWithData) {
    // Create test services
    CreateTestService(VEHICLE_ID, "fleet_service_a", "1.0.0");
    CreateTestService(VEHICLE_ID, "fleet_service_b", "1.0.0");
    CreateTestService(VEHICLE_ID_2, "fleet_service_a", "1.1.0");

    grpc::ClientContext context;
    proto::get_fleet_service_stats_request request;
    proto::get_fleet_service_stats_response response;

    request.mutable_filter()->set_fleet_id_filter(FLEET_ID);

    auto status = get_fleet_service_stats_stub_->get_fleet_service_stats(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "GetFleetServiceStats failed: " << status.error_message();
    EXPECT_GE(response.result().total_vehicles(), 2);
    EXPECT_GE(response.result().total_services(), 3);

    LOG(INFO) << "GetFleetServiceStats: total_vehicles=" << response.result().total_vehicles()
              << " online_vehicles=" << response.result().online_vehicles()
              << " total_services=" << response.result().total_services();

    for (const auto& stat : response.result().stats()) {
        LOG(INFO) << "  - " << stat.service_name()
                  << ": vehicles=" << stat.vehicle_count()
                  << " available=" << stat.available_count();
    }

    CleanupTestServices();
}

// =============================================================================
// FindVehiclesWithService Tests
// =============================================================================

TEST_F(DiscoveryE2ETest, FindVehiclesWithServiceBasic) {
    // Create test services
    CreateTestService(VEHICLE_ID, "find_test_service", "1.0.0");
    CreateTestService(VEHICLE_ID_2, "find_test_service", "1.0.0");

    grpc::ClientContext context;
    proto::find_vehicles_with_service_request request;
    proto::find_vehicles_with_service_response response;

    auto* filter = request.mutable_filter();
    filter->set_service_name("find_test_service");
    filter->set_page_size(100);

    auto status = find_vehicles_with_service_stub_->find_vehicles_with_service(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "FindVehiclesWithService failed: " << status.error_message();
    EXPECT_GE(response.result().total_count(), 2) << "Expected at least 2 vehicles with service";

    LOG(INFO) << "FindVehiclesWithService (find_test_service): total=" << response.result().total_count();

    for (const auto& v : response.result().vehicles()) {
        LOG(INFO) << "  - " << v.vehicle_id() << " (online=" << v.is_online() << ")";
    }

    CleanupTestServices();
}

TEST_F(DiscoveryE2ETest, FindVehiclesWithServiceAvailableOnly) {
    // Note: With hash-based protocol, there is no runtime status tracking.
    // All registered services are considered "available".
    // This test verifies that available_only=true still returns all services.
    CreateTestService(VEHICLE_ID, "status_test_service", "1.0.0", "available");
    CreateTestService(VEHICLE_ID_2, "status_test_service", "1.0.0", "unavailable");

    grpc::ClientContext context;
    proto::find_vehicles_with_service_request request;
    proto::find_vehicles_with_service_response response;

    auto* filter = request.mutable_filter();
    filter->set_service_name("status_test_service");
    filter->set_available_only(true);
    filter->set_page_size(100);

    auto status = find_vehicles_with_service_stub_->find_vehicles_with_service(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "FindVehiclesWithService (available_only) failed: " << status.error_message();

    LOG(INFO) << "FindVehiclesWithService (available_only): total=" << response.result().total_count();

    // With new schema, all registered services are considered "available"
    // Both vehicles should be found
    bool found_v1 = false, found_v2 = false;
    for (const auto& v : response.result().vehicles()) {
        if (v.vehicle_id() == VEHICLE_ID) found_v1 = true;
        if (v.vehicle_id() == VEHICLE_ID_2) found_v2 = true;
    }
    EXPECT_TRUE(found_v1) << "Vehicle 1 not found";
    EXPECT_TRUE(found_v2) << "Vehicle 2 not found";

    CleanupTestServices();
}

TEST_F(DiscoveryE2ETest, FindVehiclesWithServiceMissingServiceName) {
    grpc::ClientContext context;
    proto::find_vehicles_with_service_request request;
    proto::find_vehicles_with_service_response response;

    // Don't set service_name

    auto status = find_vehicles_with_service_stub_->find_vehicles_with_service(&context, request, &response);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT);
}

TEST_F(DiscoveryE2ETest, FindVehiclesWithServiceNotFound) {
    grpc::ClientContext context;
    proto::find_vehicles_with_service_request request;
    proto::find_vehicles_with_service_response response;

    auto* filter = request.mutable_filter();
    filter->set_service_name("nonexistent_service_xyz_12345");
    filter->set_page_size(100);

    auto status = find_vehicles_with_service_stub_->find_vehicles_with_service(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "FindVehiclesWithService failed: " << status.error_message();
    EXPECT_EQ(response.result().total_count(), 0);
    EXPECT_EQ(response.result().vehicles_size(), 0);

    LOG(INFO) << "FindVehiclesWithService (not found): total=" << response.result().total_count();
}

// =============================================================================
// Real Vehicle Tests (requires ifex-vehicle:latest Docker image)
// =============================================================================

/**
 * @brief E2E tests with real vehicle container for discovery sync
 *
 * These tests verify the complete discovery sync flow:
 *   Vehicle Discovery → Backend Transport → MQTT → mqtt_kafka_bridge → Kafka
 *                                                                         ↓
 *   Cloud Discovery API ← PostgreSQL ← discovery_mirror ←─────────────────┘
 *
 * Prerequisites:
 *   - Docker image: ifex-vehicle:latest (build with covesa-ifex-core/build-test-container.sh)
 *   - Infrastructure: postgres, kafka, mosquitto (run deploy/start-infra.sh)
 */
class DiscoveryRealVehicleTest : public ifex::offboard::test::E2ETestFixture {
protected:
    static constexpr const char* DISCOVERY_ADDR = "localhost:50081";
    // IFEX-based stubs
    static std::unique_ptr<proto::list_vehicles_service::Stub> list_vehicles_stub_;
    static std::unique_ptr<proto::get_vehicle_services_service::Stub> get_vehicle_services_stub_;
    static std::unique_ptr<proto::query_services_by_name_service::Stub> query_services_by_name_stub_;
    static std::unique_ptr<proto::find_vehicles_with_service_service::Stub> find_vehicles_with_service_stub_;
    static std::shared_ptr<grpc::Channel> channel_;
    static pid_t discovery_api_pid_;

    static void SetUpTestSuite() {
        // Start E2E infrastructure (vehicle, bridges)
        ifex::offboard::test::E2ETestFixture::SetUpTestSuite();

        // Wait for echo_service to register (indicates discovery sync is working)
        ASSERT_TRUE(ifex::offboard::test::E2ETestInfrastructure::WaitForServiceRegistered(
            ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID,
            "echo_service",
            std::chrono::seconds(30)))
            << "Echo service did not register within 30 seconds";

        // Start discovery_api with E2E isolated ports
        std::string binary = ifex::offboard::test::E2ETestInfrastructure::GetBinaryPath("discovery_api");
        std::string pg_port = std::to_string(ifex::offboard::test::E2ETestInfrastructure::POSTGRES_PORT);

        discovery_api_pid_ = fork();
        if (discovery_api_pid_ == 0) {
            execl(binary.c_str(), "discovery_api",
                  "--grpc_listen", DISCOVERY_ADDR,
                  "--postgres_host", "localhost",
                  "--postgres_port", pg_port.c_str(),
                  "--postgres_db", "ifex_offboard",
                  "--postgres_user", "ifex",
                  "--postgres_password", "ifex_dev",
                  "--logtostderr",
                  nullptr);
            LOG(ERROR) << "Failed to exec discovery_api";
            _exit(1);
        }
        ASSERT_GT(discovery_api_pid_, 0) << "Failed to fork discovery_api";
        LOG(INFO) << "Started discovery_api with PID " << discovery_api_pid_;

        // Connect gRPC client
        std::this_thread::sleep_for(std::chrono::seconds(2));
        channel_ = grpc::CreateChannel(DISCOVERY_ADDR, grpc::InsecureChannelCredentials());
        list_vehicles_stub_ = proto::list_vehicles_service::NewStub(channel_);
        get_vehicle_services_stub_ = proto::get_vehicle_services_service::NewStub(channel_);
        query_services_by_name_stub_ = proto::query_services_by_name_service::NewStub(channel_);
        find_vehicles_with_service_stub_ = proto::find_vehicles_with_service_service::NewStub(channel_);

        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
        ASSERT_TRUE(channel_->WaitForConnected(deadline))
            << "Failed to connect to discovery_api";

        LOG(INFO) << "Real vehicle discovery test infrastructure ready";
    }

    static void TearDownTestSuite() {
        // Close gRPC first
        list_vehicles_stub_.reset();
        get_vehicle_services_stub_.reset();
        query_services_by_name_stub_.reset();
        find_vehicles_with_service_stub_.reset();
        channel_.reset();

        // Stop discovery_api
        if (discovery_api_pid_ > 0) {
            LOG(INFO) << "Stopping discovery_api (PID " << discovery_api_pid_ << ")";
            kill(discovery_api_pid_, SIGTERM);
            int status;
            waitpid(discovery_api_pid_, &status, 0);
            discovery_api_pid_ = 0;
        }

        // Stop E2E infrastructure
        ifex::offboard::test::E2ETestFixture::TearDownTestSuite();
    }
};

// Static members
std::unique_ptr<proto::list_vehicles_service::Stub> DiscoveryRealVehicleTest::list_vehicles_stub_;
std::unique_ptr<proto::get_vehicle_services_service::Stub> DiscoveryRealVehicleTest::get_vehicle_services_stub_;
std::unique_ptr<proto::query_services_by_name_service::Stub> DiscoveryRealVehicleTest::query_services_by_name_stub_;
std::unique_ptr<proto::find_vehicles_with_service_service::Stub> DiscoveryRealVehicleTest::find_vehicles_with_service_stub_;
std::shared_ptr<grpc::Channel> DiscoveryRealVehicleTest::channel_;
pid_t DiscoveryRealVehicleTest::discovery_api_pid_ = 0;

TEST_F(DiscoveryRealVehicleTest, RealVehicle_EchoServiceDiscovered) {
    // NOTE: Test is DISABLED until vehicle discovery sync is verified working
    // Enable once vehicle infrastructure is confirmed

    // Vehicle auto-registers echo_service on startup
    // Sync flow: vehicle → MQTT → mqtt_kafka_bridge → discovery_mirror → PostgreSQL

    const char* vehicle_id = ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID;

    // Query via Discovery API
    grpc::ClientContext context;
    proto::get_vehicle_services_request request;
    request.set_vehicle_id(vehicle_id);

    proto::get_vehicle_services_response response;
    auto status = get_vehicle_services_stub_->get_vehicle_services(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "GetVehicleServices failed: " << status.error_message();
    ASSERT_GE(response.result().services_size(), 1) << "Expected at least 1 service from real vehicle";

    bool found_echo = false;
    for (const auto& svc : response.result().services()) {
        LOG(INFO) << "Found service on real vehicle: " << svc.service_name()
                  << " v" << (svc.version().empty() ? "(none)" : svc.version());
        if (svc.service_name() == "echo_service") {
            found_echo = true;
            // Version may be empty if not defined in IFEX schema
            // Just log it for debugging
            if (svc.version().empty()) {
                LOG(WARNING) << "echo_service has no version defined in IFEX schema";
            }
        }
    }
    EXPECT_TRUE(found_echo) << "echo_service not found on real vehicle";
}

TEST_F(DiscoveryRealVehicleTest, RealVehicle_VehicleIsOnline) {
    // Verify the real vehicle appears online in the fleet list

    const char* vehicle_id = ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID;

    grpc::ClientContext context;
    proto::list_vehicles_request request;
    auto* filter = request.mutable_filter();
    filter->set_online_only(true);
    filter->set_page_size(100);

    proto::list_vehicles_response response;
    auto status = list_vehicles_stub_->list_vehicles(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "ListVehicles failed: " << status.error_message();

    bool found_vehicle = false;
    for (const auto& v : response.result().vehicles()) {
        if (v.vehicle_id() == vehicle_id) {
            found_vehicle = true;
            EXPECT_TRUE(v.is_online()) << "Real vehicle should be online";
            LOG(INFO) << "Found real vehicle: " << v.vehicle_id()
                      << " online=" << v.is_online()
                      << " services=" << v.service_count();
            break;
        }
    }
    EXPECT_TRUE(found_vehicle) << "Real vehicle not found in online list";
}

TEST_F(DiscoveryRealVehicleTest, RealVehicle_QueryByServiceName) {
    // Query fleet-wide for echo_service - should find our real vehicle

    grpc::ClientContext context;
    proto::query_services_by_name_request request;
    auto* filter = request.mutable_filter();
    filter->set_service_name_pattern("echo_service");
    filter->set_page_size(100);

    proto::query_services_by_name_response response;
    auto status = query_services_by_name_stub_->query_services_by_name(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "QueryServicesByName failed: " << status.error_message();
    EXPECT_GE(response.result().total_count(), 1) << "Expected at least 1 vehicle with echo_service";

    const char* vehicle_id = ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID;

    bool found_our_vehicle = false;
    for (const auto& loc : response.result().locations()) {
        LOG(INFO) << "Found echo_service on: " << loc.vehicle_id()
                  << " v" << loc.service().version();
        if (loc.vehicle_id() == vehicle_id) {
            found_our_vehicle = true;
            EXPECT_EQ(loc.service().service_name(), "echo_service");
        }
    }
    EXPECT_TRUE(found_our_vehicle) << "Our real vehicle not found in echo_service query";
}

TEST_F(DiscoveryRealVehicleTest, RealVehicle_FindVehiclesWithService) {
    // Find all vehicles with echo_service

    grpc::ClientContext context;
    proto::find_vehicles_with_service_request request;
    auto* filter = request.mutable_filter();
    filter->set_service_name("echo_service");
    filter->set_page_size(100);

    proto::find_vehicles_with_service_response response;
    auto status = find_vehicles_with_service_stub_->find_vehicles_with_service(&context, request, &response);

    ASSERT_TRUE(status.ok()) << "FindVehiclesWithService failed: " << status.error_message();
    EXPECT_GE(response.result().total_count(), 1) << "Expected at least 1 vehicle with echo_service";

    const char* vehicle_id = ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID;

    bool found_our_vehicle = false;
    for (const auto& v : response.result().vehicles()) {
        LOG(INFO) << "Vehicle with echo_service: " << v.vehicle_id()
                  << " online=" << v.is_online();
        if (v.vehicle_id() == vehicle_id) {
            found_our_vehicle = true;
            EXPECT_TRUE(v.is_online());
        }
    }
    EXPECT_TRUE(found_our_vehicle) << "Our real vehicle not found";
}

TEST_F(DiscoveryRealVehicleTest, RealVehicle_ServiceSchemaAvailable) {
    // Verify that service schema (IFEX YAML) was synced from vehicle

    auto* db = ifex::offboard::test::E2ETestInfrastructure::GetDatabase();
    ASSERT_NE(db, nullptr);

    const char* vehicle_id = ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID;

    // Check if schema was stored (via hash-based sync protocol)
    auto result = db->execute(R"(
        SELECT sr.service_name, sr.version, sr.schema_hash
        FROM vehicle_schemas vs
        JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash
        WHERE vs.vehicle_id = $1 AND sr.service_name = 'echo_service'
    )", {vehicle_id});

    ASSERT_TRUE(result.ok()) << "Failed to query vehicle_schemas: " << result.error();
    ASSERT_GE(result.num_rows(), 1) << "echo_service not found in vehicle_schemas table";

    LOG(INFO) << "Found echo_service schema via hash-based sync";
    std::string hash = result.row(0).get_string("schema_hash");
    std::string version = result.row(0).get_string("version");
    LOG(INFO) << "  hash=" << hash << " version=" << version;
    EXPECT_FALSE(hash.empty());
}
