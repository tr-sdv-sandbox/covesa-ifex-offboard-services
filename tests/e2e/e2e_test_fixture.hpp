/**
 * @file e2e_test_fixture.hpp
 * @brief Shared fixture for E2E tests with real vehicle containers
 *
 * Provides infrastructure management for true end-to-end testing:
 * - Starts/stops Docker containers (vehicle with echo service)
 * - Starts/stops bridge services (mqtt_kafka_bridge, mirrors)
 * - Waits for vehicle registration and service discovery
 * - Provides helper methods for test assertions
 *
 * Prerequisites:
 *   - Docker infrastructure running (postgres, kafka, mosquitto)
 *   - ifex-vehicle:latest image built from covesa-ifex-core
 *
 * Run: ./deploy/start-infra.sh && ctest -L e2e
 */

#pragma once

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "postgres_client.hpp"

namespace ifex::offboard::test {

/**
 * @brief Shared infrastructure for E2E tests with real vehicle containers
 *
 * SetUpTestSuite starts:
 *   1. mqtt_kafka_bridge (routes MQTT ↔ Kafka)
 *   2. discovery_mirror (syncs discovery to PostgreSQL)
 *   3. scheduler_mirror (syncs jobs to PostgreSQL)
 *   4. ifex-vehicle Docker container with echo service
 *
 * TearDownTestSuite stops all services and cleans up.
 */
class E2ETestInfrastructure {
public:
    // Configuration
    static constexpr const char* VEHICLE_IMAGE = "ifex-vehicle:latest";
    static constexpr const char* VEHICLE_CONTAINER_NAME = "e2e-test-vehicle";
    static constexpr const char* VEHICLE_ID = "E2E_TEST_VIN_001";
    static constexpr const char* MQTT_HOST = "localhost";
    static constexpr int MQTT_PORT = 1883;
    static constexpr const char* KAFKA_BROKER = "localhost:9092";
    static constexpr const char* POSTGRES_HOST = "localhost";
    static constexpr int POSTGRES_PORT = 5432;
    static constexpr const char* POSTGRES_DB = "ifex_offboard";
    static constexpr const char* POSTGRES_USER = "ifex";
    static constexpr const char* POSTGRES_PASSWORD = "ifex_dev";

    /**
     * @brief Start all E2E infrastructure
     * @param need_vehicle If true (default), verify and start vehicle container
     * @return true if all services started successfully
     */
    static bool StartInfrastructure(bool need_vehicle = true);

    /**
     * @brief Stop all E2E infrastructure
     */
    static void StopInfrastructure();

    /**
     * @brief Check if infrastructure is running
     */
    static bool IsRunning() { return is_running_; }

    /**
     * @brief Get PostgreSQL client for test assertions
     */
    static PostgresClient* GetDatabase() { return db_.get(); }

    /**
     * @brief Wait for vehicle to appear online in database
     * @param vehicle_id Vehicle identifier
     * @param timeout Maximum wait time
     * @return true if vehicle is online within timeout
     */
    static bool WaitForVehicleOnline(const std::string& vehicle_id,
                                      std::chrono::seconds timeout = std::chrono::seconds(30));

    /**
     * @brief Wait for a service to be registered in discovery
     * @param vehicle_id Vehicle identifier
     * @param service_name Service name to wait for
     * @param timeout Maximum wait time
     * @return true if service is registered within timeout
     */
    static bool WaitForServiceRegistered(const std::string& vehicle_id,
                                          const std::string& service_name,
                                          std::chrono::seconds timeout = std::chrono::seconds(30));

    /**
     * @brief Wait for job execution to appear in database
     * @param job_id Job identifier
     * @param timeout Maximum wait time
     * @return true if execution record found within timeout
     */
    static bool WaitForJobExecution(const std::string& job_id,
                                     std::chrono::seconds timeout = std::chrono::seconds(30));

    /**
     * @brief Get the executable directory (where test binaries are)
     */
    static std::string GetExecutableDir();

    /**
     * @brief Get path to a service binary (in build root)
     */
    static std::string GetBinaryPath(const std::string& name);

    /**
     * @brief Reset database schema (drop all tables and recreate from schema.sql)
     * @param db PostgreSQL client to use (optional, uses internal client if null)
     */
    static void ResetDatabase(PostgresClient* db = nullptr);

private:
    static bool StartBridgeServices();
    static void StopBridgeServices();
    static bool StartVehicleContainer();
    static void StopVehicleContainer();
    static bool VerifyDockerInfrastructure();
    static bool VerifyVehicleImageExists();
    static bool CreateKafkaTopics();

    // Process IDs for bridge services
    static pid_t mqtt_kafka_bridge_pid_;
    static pid_t discovery_mirror_pid_;
    static pid_t scheduler_mirror_pid_;

    // Database client
    static std::unique_ptr<PostgresClient> db_;

    // State
    static bool is_running_;
};

/**
 * @brief Base fixture for E2E tests requiring vehicle infrastructure
 *
 * Inherit from this fixture for tests that need:
 * - Real vehicle container running echo service
 * - mqtt_kafka_bridge routing messages
 * - discovery_mirror and scheduler_mirror syncing to PostgreSQL
 *
 * Example:
 *   class MyE2ETest : public E2ETestFixture { ... };
 *   TEST_F(MyE2ETest, RealVehicleTest) { ... }
 */
class E2ETestFixture : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        // Start infrastructure if not already running
        if (!E2ETestInfrastructure::IsRunning()) {
            ASSERT_TRUE(E2ETestInfrastructure::StartInfrastructure())
                << "Failed to start E2E infrastructure";
        }

        // Wait for vehicle to be online
        ASSERT_TRUE(E2ETestInfrastructure::WaitForVehicleOnline(
            E2ETestInfrastructure::VEHICLE_ID, std::chrono::seconds(30)))
            << "Vehicle did not come online within 30 seconds";

        LOG(INFO) << "E2E infrastructure ready with vehicle: "
                  << E2ETestInfrastructure::VEHICLE_ID;
    }

    static void TearDownTestSuite() {
        // Always stop infrastructure at end of test suite to ensure clean state
        // The atexit handler is a backup for crashes/interrupts
        E2ETestInfrastructure::StopInfrastructure();
    }

    void SetUp() override {
        ASSERT_TRUE(E2ETestInfrastructure::IsRunning())
            << "E2E infrastructure not running";
    }

    // Helper to get database client
    PostgresClient* db() {
        return E2ETestInfrastructure::GetDatabase();
    }

    // Helper to get vehicle ID
    static const char* vehicle_id() {
        return E2ETestInfrastructure::VEHICLE_ID;
    }
};

}  // namespace ifex::offboard::test
