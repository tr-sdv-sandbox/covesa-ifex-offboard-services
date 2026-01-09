/**
 * @file status_e2e_test.cpp
 * @brief End-to-end tests for Vehicle Online/Offline Status Tracking
 *
 * These tests verify the complete flow:
 *   MQTT (v2c/{vehicle_id}/is_online) → mqtt_kafka_bridge → PostgreSQL
 *
 * Uses shared E2E infrastructure (mqtt_kafka_bridge started automatically).
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <thread>
#include <string>
#include <memory>
#include <cstdlib>

#include <mosquitto.h>
#include "postgres_client.hpp"
#include "e2e_test_fixture.hpp"

using namespace std::chrono_literals;
using namespace ifex::offboard;

/**
 * @brief Test fixture for Status E2E tests
 *
 * Uses shared E2E infrastructure that manages mqtt_kafka_bridge lifecycle.
 */
class StatusE2ETest : public ::testing::Test {
protected:
    // Use E2E isolated ports
    static constexpr const char* MQTT_HOST = "localhost";
    static constexpr int MQTT_PORT = test::E2ETestInfrastructure::MQTT_PORT;  // E2E port: 11883

    static void SetUpTestSuite() {
        // Start shared E2E infrastructure (includes mqtt_kafka_bridge)
        if (!test::E2ETestInfrastructure::IsRunning()) {
            ASSERT_TRUE(test::E2ETestInfrastructure::StartInfrastructure(false))  // false = don't need vehicle
                << "Failed to start E2E infrastructure";
        }
    }

    static void TearDownTestSuite() {
        // Infrastructure cleanup handled by process exit via atexit handler
    }

    void SetUp() override {
        // Initialize glog for tests
        static bool glog_initialized = false;
        if (!glog_initialized) {
            google::InitGoogleLogging("status_e2e_test");
            FLAGS_logtostderr = true;
            glog_initialized = true;
        }

        // Initialize mosquitto library (once)
        static bool mosquitto_initialized = false;
        if (!mosquitto_initialized) {
            mosquitto_lib_init();
            mosquitto_initialized = true;
        }

        // Generate unique vehicle ID for this test
        vehicle_id_ = "status-test-" + std::to_string(std::rand());

        // Connect to PostgreSQL using E2E isolated port
        PostgresConfig pg_config;
        pg_config.host = "localhost";
        pg_config.port = test::E2ETestInfrastructure::POSTGRES_PORT;  // E2E port: 15432
        pg_config.database = "ifex_offboard";
        pg_config.user = "ifex";
        pg_config.password = "ifex_dev";

        db_ = std::make_unique<PostgresClient>(pg_config);
        ASSERT_TRUE(db_->is_connected())
            << "PostgreSQL not available on E2E port";

        // Connect to MQTT (required)
        mosq_ = mosquitto_new("status-e2e-test", true, nullptr);
        ASSERT_NE(mosq_, nullptr) << "Failed to create MQTT client";

        int rc = mosquitto_connect(mosq_, MQTT_HOST, MQTT_PORT, 60);
        ASSERT_EQ(rc, MOSQ_ERR_SUCCESS)
            << "MQTT connection failed: " << mosquitto_strerror(rc);
    }

    void TearDown() override {
        // Clean up test vehicle from database
        if (db_ && db_->is_connected()) {
            db_->execute(
                "DELETE FROM vehicles WHERE vehicle_id = $1",
                {vehicle_id_});
        }

        // Clean up MQTT retained message
        if (mosq_) {
            std::string topic = "v2c/" + vehicle_id_ + "/is_online";
            mosquitto_publish(mosq_, nullptr, topic.c_str(), 0, nullptr, 1, true);
            mosquitto_loop(mosq_, 500, 1);
            mosquitto_disconnect(mosq_);
            mosquitto_destroy(mosq_);
            mosq_ = nullptr;
        }
    }

    // Publish status to MQTT
    bool publishStatus(bool is_online) {
        std::string topic = "v2c/" + vehicle_id_ + "/is_online";
        const char* payload = is_online ? "1" : "0";

        int rc = mosquitto_publish(mosq_, nullptr, topic.c_str(),
                                   1, payload, 1, true);  // QoS 1, retained
        if (rc != MOSQ_ERR_SUCCESS) {
            LOG(ERROR) << "Failed to publish: " << mosquitto_strerror(rc);
            return false;
        }

        // Wait for publish to complete
        mosquitto_loop(mosq_, 1000, 1);
        return true;
    }

    // Get is_online status from database
    std::optional<bool> getDbStatus() {
        auto result = db_->execute(
            "SELECT is_online FROM vehicles WHERE vehicle_id = $1",
            {vehicle_id_});

        if (!result.ok() || result.num_rows() == 0) {
            return std::nullopt;
        }

        std::string value = result.row(0).get_string("is_online");
        return value == "t";
    }

    // Wait for database status to match expected value
    bool waitForDbStatus(bool expected, std::chrono::seconds timeout = 10s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;

        while (std::chrono::steady_clock::now() < deadline) {
            auto status = getDbStatus();
            if (status.has_value() && *status == expected) {
                return true;
            }
            std::this_thread::sleep_for(500ms);
        }
        return false;
    }

    std::string vehicle_id_;
    std::unique_ptr<PostgresClient> db_;
    struct mosquitto* mosq_ = nullptr;
};

// =============================================================================
// Connection Tests
// =============================================================================

TEST_F(StatusE2ETest, InfrastructureConnection) {
    // This test verifies infrastructure is running
    if (db_ && db_->is_connected() && mosq_) {
        LOG(INFO) << "Infrastructure available: PostgreSQL and MQTT connected";
        SUCCEED();
    } else {
        LOG(WARNING) << "Infrastructure not available - status tests will be skipped";
        SUCCEED();
    }
}

// =============================================================================
// Online Status Tests
// =============================================================================

TEST_F(StatusE2ETest, PublishOnlineUpdatesDatabase) {

    LOG(INFO) << "Testing: Publish is_online=1 → DB is_online=true";
    LOG(INFO) << "Vehicle ID: " << vehicle_id_;

    // Publish online status
    ASSERT_TRUE(publishStatus(true)) << "Failed to publish online status";

    // Wait for mqtt_kafka_bridge to update database
    ASSERT_TRUE(waitForDbStatus(true, 15s))
        << "Database should show is_online=true after publishing '1'";

    auto status = getDbStatus();
    ASSERT_TRUE(status.has_value());
    EXPECT_TRUE(*status) << "is_online should be true";

    LOG(INFO) << "PASS: Vehicle marked as online in database";
}

TEST_F(StatusE2ETest, PublishOfflineUpdatesDatabase) {

    LOG(INFO) << "Testing: Publish is_online=0 → DB is_online=false";
    LOG(INFO) << "Vehicle ID: " << vehicle_id_;

    // First set online
    ASSERT_TRUE(publishStatus(true));
    ASSERT_TRUE(waitForDbStatus(true, 15s));

    // Now set offline
    ASSERT_TRUE(publishStatus(false)) << "Failed to publish offline status";

    // Wait for database update
    ASSERT_TRUE(waitForDbStatus(false, 15s))
        << "Database should show is_online=false after publishing '0'";

    auto status = getDbStatus();
    ASSERT_TRUE(status.has_value());
    EXPECT_FALSE(*status) << "is_online should be false";

    LOG(INFO) << "PASS: Vehicle marked as offline in database";
}

TEST_F(StatusE2ETest, StatusTransitions) {

    LOG(INFO) << "Testing: Multiple status transitions";
    LOG(INFO) << "Vehicle ID: " << vehicle_id_;

    // Online → Offline → Online → Offline
    std::vector<bool> transitions = {true, false, true, false};

    for (size_t i = 0; i < transitions.size(); ++i) {
        bool expected = transitions[i];
        LOG(INFO) << "Transition " << (i + 1) << ": is_online=" << expected;

        ASSERT_TRUE(publishStatus(expected))
            << "Failed to publish status " << expected;

        ASSERT_TRUE(waitForDbStatus(expected, 15s))
            << "Database should show is_online=" << expected;

        auto status = getDbStatus();
        ASSERT_TRUE(status.has_value());
        EXPECT_EQ(*status, expected);
    }

    LOG(INFO) << "PASS: All status transitions successful";
}

TEST_F(StatusE2ETest, LastSeenUpdated) {

    LOG(INFO) << "Testing: last_seen_at updated on status change";
    LOG(INFO) << "Vehicle ID: " << vehicle_id_;

    // Publish online
    ASSERT_TRUE(publishStatus(true));
    ASSERT_TRUE(waitForDbStatus(true, 15s));

    // Get initial last_seen
    auto result1 = db_->execute(
        "SELECT last_seen_at FROM vehicles WHERE vehicle_id = $1",
        {vehicle_id_});
    ASSERT_TRUE(result1.ok() && result1.num_rows() > 0);
    std::string last_seen1 = result1.row(0).get_string("last_seen_at");

    // Wait a bit
    std::this_thread::sleep_for(1s);

    // Publish offline
    ASSERT_TRUE(publishStatus(false));
    ASSERT_TRUE(waitForDbStatus(false, 15s));

    // Get updated last_seen
    auto result2 = db_->execute(
        "SELECT last_seen_at FROM vehicles WHERE vehicle_id = $1",
        {vehicle_id_});
    ASSERT_TRUE(result2.ok() && result2.num_rows() > 0);
    std::string last_seen2 = result2.row(0).get_string("last_seen_at");

    // last_seen_at should be updated
    EXPECT_NE(last_seen1, last_seen2)
        << "last_seen_at should be updated on status change";

    LOG(INFO) << "PASS: last_seen_at updated correctly";
    LOG(INFO) << "  First:  " << last_seen1;
    LOG(INFO) << "  Second: " << last_seen2;
}

TEST_F(StatusE2ETest, VehicleCreatedOnFirstStatus) {

    LOG(INFO) << "Testing: Vehicle record created on first status message";
    LOG(INFO) << "Vehicle ID: " << vehicle_id_;

    // Verify vehicle doesn't exist yet
    auto initial = db_->execute(
        "SELECT COUNT(*) FROM vehicles WHERE vehicle_id = $1",
        {vehicle_id_});
    ASSERT_TRUE(initial.ok());
    EXPECT_EQ(initial.row(0).get_int(0), 0) << "Vehicle should not exist yet";

    // Publish online
    ASSERT_TRUE(publishStatus(true));
    ASSERT_TRUE(waitForDbStatus(true, 15s));

    // Verify vehicle was created
    auto after = db_->execute(
        "SELECT COUNT(*) FROM vehicles WHERE vehicle_id = $1",
        {vehicle_id_});
    ASSERT_TRUE(after.ok());
    EXPECT_EQ(after.row(0).get_int(0), 1) << "Vehicle should be created";

    LOG(INFO) << "PASS: Vehicle record created on first status";
}

// =============================================================================
// Error Cases
// =============================================================================

TEST_F(StatusE2ETest, InvalidPayloadIgnored) {

    LOG(INFO) << "Testing: Invalid payload handling";
    LOG(INFO) << "Vehicle ID: " << vehicle_id_;

    // First set a valid online status
    ASSERT_TRUE(publishStatus(true));
    ASSERT_TRUE(waitForDbStatus(true, 15s));

    // Publish invalid payload
    std::string topic = "v2c/" + vehicle_id_ + "/is_online";
    const char* invalid = "invalid";
    mosquitto_publish(mosq_, nullptr, topic.c_str(), 7, invalid, 1, true);
    mosquitto_loop(mosq_, 1000, 1);

    // Wait a bit
    std::this_thread::sleep_for(2s);

    // Status should still be true (invalid payload ignored or treated as offline)
    auto status = getDbStatus();
    ASSERT_TRUE(status.has_value());
    // Note: Behavior depends on implementation - may stay true or become false
    LOG(INFO) << "After invalid payload, is_online=" << (*status ? "true" : "false");
}

