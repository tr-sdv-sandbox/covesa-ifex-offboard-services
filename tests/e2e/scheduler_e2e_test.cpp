/**
 * @file scheduler_e2e_test.cpp
 * @brief End-to-end tests for Cloud Scheduler API with REAL vehicle container
 *
 * These tests verify the COMPLETE cloud→vehicle→cloud flow:
 * 1. Cloud scheduler_api creates job, publishes to Kafka
 * 2. mqtt_kafka_bridge routes to MQTT
 * 3. REAL vehicle container receives command, processes it
 * 4. Vehicle scheduler syncs back via MQTT → Kafka → scheduler_mirror
 * 5. Test verifies sync_state changes from 'pending' to 'synced'
 *
 * Prerequisites:
 *   - E2E Docker infrastructure: docker compose -f tests/e2e/docker-compose.e2e.yml up -d
 *   - Vehicle image: cd ../covesa-ifex-core && ./build-test-container.sh
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <thread>
#include <string>
#include <sstream>
#include <iomanip>
#include <memory>
#include <atomic>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#include <linux/limits.h>

#include <grpcpp/grpcpp.h>
#include "cloud-scheduler-service.grpc.pb.h"
#include "cloud-dispatcher-service.grpc.pb.h"
#include "postgres_client.hpp"
#include "e2e_test_fixture.hpp"
#include <nlohmann/json.hpp>

// IFEX-based proto namespace
namespace proto = swdv::cloud_scheduler_service;
namespace dispatcher_proto = swdv::cloud_dispatcher_service;
using ifex::offboard::test::E2ETestInfrastructure;
using json = nlohmann::json;

using namespace std::chrono_literals;

// Helper to convert ISO8601 string to epoch milliseconds
static uint64_t Iso8601ToMs(const std::string& iso_str) {
    if (iso_str.empty()) return 0;
    std::tm tm = {};
    std::istringstream ss(iso_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (ss.fail()) return 0;
    auto tp = std::chrono::system_clock::from_time_t(timegm(&tm));
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch()).count();
}

/**
 * @brief Test fixture for Scheduler E2E tests with REAL vehicle container
 *
 * Starts:
 * 1. E2E infrastructure (mqtt_kafka_bridge, discovery_mirror, scheduler_mirror, vehicle)
 * 2. scheduler_api (configured for E2E Kafka topics)
 * 3. dispatcher_api (for querying vehicle's scheduler directly)
 *
 * The flow: scheduler_api → Kafka → mqtt_kafka_bridge → MQTT → vehicle → sync back
 */
class SchedulerE2ETest : public ::testing::Test {
protected:
    static constexpr const char* CLOUD_SCHEDULER_ADDR = "localhost:50083";
    static constexpr const char* CLOUD_DISPATCHER_ADDR = "localhost:50082";

    // Use the E2E vehicle ID (matches vehicle container started by E2ETestInfrastructure)
    static constexpr const char* VEHICLE_ID = "E2E_TEST_VIN_001";

    // Service processes
    static pid_t scheduler_pid_;
    static pid_t dispatcher_pid_;

    static void SetUpTestSuite() {
        // Initialize glog
        static bool glog_initialized = false;
        if (!glog_initialized) {
            google::InitGoogleLogging("scheduler_e2e_test");
            FLAGS_logtostderr = true;
            glog_initialized = true;
        }

        // Start E2E infrastructure (mqtt_kafka_bridge, mirrors, vehicle container)
        LOG(INFO) << "Starting E2E infrastructure with real vehicle...";
        bool infra_started = E2ETestInfrastructure::StartInfrastructure(true);  // true = need vehicle
        ASSERT_TRUE(infra_started)
            << "Failed to start E2E infrastructure. Check prerequisites:\n"
            << "  1. docker compose -f tests/e2e/docker-compose.e2e.yml up -d\n"
            << "  2. cd ../covesa-ifex-core && ./build-test-container.sh";

        // Wait for vehicle to come online
        LOG(INFO) << "Waiting for vehicle " << VEHICLE_ID << " to come online...";
        bool vehicle_online = E2ETestInfrastructure::WaitForVehicleOnline(VEHICLE_ID, 30s);
        ASSERT_TRUE(vehicle_online) << "Vehicle did not come online within 30 seconds";

        // Start scheduler_api service (configured for E2E Kafka topics)
        LOG(INFO) << "Starting scheduler_api with E2E Kafka topic...";
        scheduler_pid_ = fork();
        if (scheduler_pid_ == 0) {
            // Child process - exec scheduler_api
            std::string binary = E2ETestInfrastructure::GetBinaryPath("scheduler_api");
            std::string kafka_arg = std::string("--kafka_broker=") + E2ETestInfrastructure::KAFKA_BROKER;
            std::string postgres_host_arg = std::string("--postgres_host=") + E2ETestInfrastructure::POSTGRES_HOST;
            std::string postgres_port_arg = "--postgres_port=" + std::to_string(E2ETestInfrastructure::POSTGRES_PORT);
            execl(binary.c_str(), "scheduler_api",
                  "--grpc_listen=0.0.0.0:50083",
                  kafka_arg.c_str(),
                  postgres_host_arg.c_str(),
                  postgres_port_arg.c_str(),
                  "--postgres_db=ifex_offboard",
                  "--postgres_user=ifex",
                  "--postgres_password=ifex_dev",
                  // Use E2E Kafka topic for c2v commands
                  "--kafka_topic_c2v=e2e.c2v.scheduler",
                  "--logtostderr",
                  nullptr);
            _exit(1);  // execl failed
        }

        ASSERT_GT(scheduler_pid_, 0) << "Failed to fork scheduler_api";
        LOG(INFO) << "Started scheduler_api with PID " << scheduler_pid_;

        // Wait for scheduler_api to be ready
        auto scheduler_channel = grpc::CreateChannel(CLOUD_SCHEDULER_ADDR, grpc::InsecureChannelCredentials());
        bool scheduler_ready = scheduler_channel->WaitForConnected(
            std::chrono::system_clock::now() + std::chrono::seconds(10));
        ASSERT_TRUE(scheduler_ready) << "scheduler_api did not become ready in time";
        LOG(INFO) << "scheduler_api is ready";

        // Start dispatcher_api service (for querying vehicle's scheduler directly)
        LOG(INFO) << "Starting dispatcher_api with E2E Kafka topics...";
        dispatcher_pid_ = fork();
        if (dispatcher_pid_ == 0) {
            std::string binary = E2ETestInfrastructure::GetBinaryPath("dispatcher_api");
            execl(binary.c_str(), "dispatcher_api",
                  "--grpc_listen", CLOUD_DISPATCHER_ADDR,
                  "--kafka_broker", E2ETestInfrastructure::KAFKA_BROKER,
                  "--kafka_topic_v2c", "e2e.rpc.200",
                  "--kafka_topic_c2v", "e2e.c2v.rpc",
                  "--logtostderr",
                  nullptr);
            LOG(ERROR) << "Failed to exec dispatcher_api";
            _exit(1);
        }

        ASSERT_GT(dispatcher_pid_, 0) << "Failed to fork dispatcher_api";
        LOG(INFO) << "Started dispatcher_api with PID " << dispatcher_pid_;

        // Wait for dispatcher_api to be ready
        std::this_thread::sleep_for(std::chrono::seconds(2));
        auto dispatcher_channel = grpc::CreateChannel(CLOUD_DISPATCHER_ADDR, grpc::InsecureChannelCredentials());
        bool dispatcher_ready = dispatcher_channel->WaitForConnected(
            std::chrono::system_clock::now() + std::chrono::seconds(10));
        ASSERT_TRUE(dispatcher_ready) << "dispatcher_api did not become ready in time";
        LOG(INFO) << "dispatcher_api is ready";

        LOG(INFO) << "========================================";
        LOG(INFO) << "E2E Scheduler Test Infrastructure Ready";
        LOG(INFO) << "  Vehicle: " << VEHICLE_ID;
        LOG(INFO) << "  Scheduler API: " << CLOUD_SCHEDULER_ADDR;
        LOG(INFO) << "  Dispatcher API: " << CLOUD_DISPATCHER_ADDR;
        LOG(INFO) << "========================================";
    }

    static void TearDownTestSuite() {
        // Stop scheduler_api
        if (scheduler_pid_ > 0) {
            kill(scheduler_pid_, SIGTERM);
            int status;
            waitpid(scheduler_pid_, &status, 0);
            LOG(INFO) << "Stopped scheduler_api (PID " << scheduler_pid_ << ")";
            scheduler_pid_ = 0;
        }

        // Stop dispatcher_api
        if (dispatcher_pid_ > 0) {
            kill(dispatcher_pid_, SIGTERM);
            int status;
            waitpid(dispatcher_pid_, &status, 0);
            LOG(INFO) << "Stopped dispatcher_api (PID " << dispatcher_pid_ << ")";
            dispatcher_pid_ = 0;
        }

        // Stop E2E infrastructure (vehicle container, bridges)
        // This also cleans up test data from database
        E2ETestInfrastructure::StopInfrastructure();
    }

    void SetUp() override {
        // Create gRPC stubs for scheduler API
        auto scheduler_channel = grpc::CreateChannel(
            CLOUD_SCHEDULER_ADDR,
            grpc::InsecureChannelCredentials());

        create_job_stub_ = proto::create_job_service::NewStub(scheduler_channel);
        update_job_stub_ = proto::update_job_service::NewStub(scheduler_channel);
        delete_job_stub_ = proto::delete_job_service::NewStub(scheduler_channel);
        pause_job_stub_ = proto::pause_job_service::NewStub(scheduler_channel);
        resume_job_stub_ = proto::resume_job_service::NewStub(scheduler_channel);
        trigger_job_stub_ = proto::trigger_job_service::NewStub(scheduler_channel);
        get_job_stub_ = proto::get_job_service::NewStub(scheduler_channel);
        list_jobs_stub_ = proto::list_jobs_service::NewStub(scheduler_channel);
        get_job_executions_stub_ = proto::get_job_executions_service::NewStub(scheduler_channel);
        create_fleet_job_stub_ = proto::create_fleet_job_service::NewStub(scheduler_channel);
        delete_fleet_job_stub_ = proto::delete_fleet_job_service::NewStub(scheduler_channel);
        get_fleet_job_stats_stub_ = proto::get_fleet_job_stats_service::NewStub(scheduler_channel);
        healthy_stub_ = proto::healthy_service::NewStub(scheduler_channel);

        // Create gRPC stub for dispatcher API (to query vehicle's scheduler)
        auto dispatcher_channel = grpc::CreateChannel(
            CLOUD_DISPATCHER_ADDR,
            grpc::InsecureChannelCredentials());

        call_method_stub_ = dispatcher_proto::call_method_service::NewStub(dispatcher_channel);

        ASSERT_NE(create_job_stub_, nullptr);
        ASSERT_NE(call_method_stub_, nullptr);
    }

    void TearDown() override {
        // Clean up any jobs created during the test
        for (const auto& job_id : created_job_ids_) {
            proto::delete_job_request request;
            request.set_vehicle_id(VEHICLE_ID);
            request.set_job_id(job_id);

            proto::delete_job_response response;
            grpc::ClientContext context;
            delete_job_stub_->delete_job(&context, request, &response);
        }
        created_job_ids_.clear();
    }

    // Helper to create a test job and track it for cleanup
    std::string CreateTestJob(const std::string& title = "Test Job",
                              const std::string& service = "echo_service",
                              const std::string& method = "echo") {
        proto::create_job_request request;
        auto* inner = request.mutable_request();
        inner->set_vehicle_id(VEHICLE_ID);
        inner->set_title(title);
        inner->set_service(service);
        inner->set_method(method);
        inner->set_scheduled_time_ms(Iso8601ToMs("2099-12-31T23:59:59Z"));

        proto::create_job_response response;
        grpc::ClientContext context;
        auto status = create_job_stub_->create_job(&context, request, &response);

        if (status.ok() && response.result().success()) {
            std::string job_id = response.result().job_id();
            created_job_ids_.push_back(job_id);
            return job_id;
        }
        return "";
    }

    // Helper to wait for job sync state to change
    bool WaitForSyncState(const std::string& job_id, const std::string& expected_state,
                          std::chrono::seconds timeout = 10s) {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            proto::get_job_request request;
            request.set_vehicle_id(VEHICLE_ID);
            request.set_job_id(job_id);

            proto::get_job_response response;
            grpc::ClientContext context;
            auto status = get_job_stub_->get_job(&context, request, &response);

            if (status.ok() && response.result().found()) {
                auto sync_state = response.result().job().sync_state();
                std::string state_str;
                switch (sync_state) {
                    case proto::SYNC_PENDING: state_str = "pending"; break;
                    case proto::SYNC_CONFIRMED: state_str = "synced"; break;
                    case proto::SYNC_FAILED: state_str = "failed"; break;
                    default: state_str = "unknown"; break;
                }
                if (state_str == expected_state) {
                    return true;
                }
            }
            std::this_thread::sleep_for(500ms);
        }
        return false;
    }

    // gRPC stubs
    std::unique_ptr<proto::create_job_service::Stub> create_job_stub_;
    std::unique_ptr<proto::update_job_service::Stub> update_job_stub_;
    std::unique_ptr<proto::delete_job_service::Stub> delete_job_stub_;
    std::unique_ptr<proto::pause_job_service::Stub> pause_job_stub_;
    std::unique_ptr<proto::resume_job_service::Stub> resume_job_stub_;
    std::unique_ptr<proto::trigger_job_service::Stub> trigger_job_stub_;
    std::unique_ptr<proto::get_job_service::Stub> get_job_stub_;
    std::unique_ptr<proto::list_jobs_service::Stub> list_jobs_stub_;
    std::unique_ptr<proto::get_job_executions_service::Stub> get_job_executions_stub_;
    std::unique_ptr<proto::create_fleet_job_service::Stub> create_fleet_job_stub_;
    std::unique_ptr<proto::delete_fleet_job_service::Stub> delete_fleet_job_stub_;
    std::unique_ptr<proto::get_fleet_job_stats_service::Stub> get_fleet_job_stats_stub_;
    std::unique_ptr<proto::healthy_service::Stub> healthy_stub_;
    std::unique_ptr<dispatcher_proto::call_method_service::Stub> call_method_stub_;

    // Track created jobs for cleanup
    std::vector<std::string> created_job_ids_;
};

// Static member initialization
pid_t SchedulerE2ETest::scheduler_pid_ = 0;
pid_t SchedulerE2ETest::dispatcher_pid_ = 0;

// =============================================================================
// Connection Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CloudSchedulerConnection) {
    // Verify we can connect to the cloud scheduler service
    ASSERT_NE(create_job_stub_, nullptr);
    ASSERT_NE(healthy_stub_, nullptr);
}

TEST_F(SchedulerE2ETest, HealthyCheck) {
    proto::healthy_request request;
    proto::healthy_response response;
    grpc::ClientContext context;

    auto status = healthy_stub_->healthy(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.is_healthy());
}

// =============================================================================
// Job CRUD Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CreateJobBasic) {
    proto::create_job_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_title("E2E Test Job - Basic");
    inner->set_service("echo_service");
    inner->set_method("echo");
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-01-05T12:00:00Z"));

    proto::create_job_response response;
    grpc::ClientContext context;

    auto status = create_job_stub_->create_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.result().success()) << "Error: " << response.result().error_message();
    EXPECT_FALSE(response.result().job_id().empty());

    // Track for cleanup
    if (response.result().success()) {
        created_job_ids_.push_back(response.result().job_id());
    }
}

TEST_F(SchedulerE2ETest, CreateJobWithRecurrence) {
    proto::create_job_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_title("E2E Test Job - Recurring");
    inner->set_service("echo_service");
    inner->set_method("echo");
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-01-05T08:00:00Z"));
    inner->set_recurrence_rule("FREQ=DAILY;INTERVAL=1");
    inner->set_end_time_ms(Iso8601ToMs("2099-12-31T23:59:59Z"));

    proto::create_job_response response;
    grpc::ClientContext context;

    auto status = create_job_stub_->create_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.result().success()) << "Error: " << response.result().error_message();

    if (response.result().success()) {
        created_job_ids_.push_back(response.result().job_id());
    }
}

TEST_F(SchedulerE2ETest, PauseJob) {
    // Create a job first
    std::string job_id = CreateTestJob("E2E Test Job - Pause");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    // Pause it
    proto::pause_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    request.set_job_id(job_id);

    proto::pause_job_response response;
    grpc::ClientContext context;

    auto status = pause_job_stub_->pause_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.result().success()) << "Error: " << response.result().error_message();
}

TEST_F(SchedulerE2ETest, ResumeJob) {
    // Create and pause a job
    std::string job_id = CreateTestJob("E2E Test Job - Resume");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    {
        proto::pause_job_request pause_req;
        pause_req.set_vehicle_id(VEHICLE_ID);
        pause_req.set_job_id(job_id);
        proto::pause_job_response pause_resp;
        grpc::ClientContext ctx;
        pause_job_stub_->pause_job(&ctx, pause_req, &pause_resp);
    }

    // Resume it
    proto::resume_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    request.set_job_id(job_id);

    proto::resume_job_response response;
    grpc::ClientContext context;

    auto status = resume_job_stub_->resume_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.result().success()) << "Error: " << response.result().error_message();
}

TEST_F(SchedulerE2ETest, TriggerJob) {
    std::string job_id = CreateTestJob("E2E Test Job - Trigger");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    proto::trigger_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    request.set_job_id(job_id);

    proto::trigger_job_response response;
    grpc::ClientContext context;

    auto status = trigger_job_stub_->trigger_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.result().success()) << "Error: " << response.result().error_message();
}

TEST_F(SchedulerE2ETest, DeleteJob) {
    std::string job_id = CreateTestJob("E2E Test Job - Delete");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    proto::delete_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    request.set_job_id(job_id);

    proto::delete_job_response response;
    grpc::ClientContext context;

    auto status = delete_job_stub_->delete_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.result().success()) << "Error: " << response.result().error_message();

    // Remove from cleanup list since we deleted it
    created_job_ids_.erase(
        std::remove(created_job_ids_.begin(), created_job_ids_.end(), job_id),
        created_job_ids_.end());
}

TEST_F(SchedulerE2ETest, GetJob) {
    std::string job_id = CreateTestJob("E2E Test Job - Get");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    proto::get_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    request.set_job_id(job_id);

    proto::get_job_response response;
    grpc::ClientContext context;

    auto status = get_job_stub_->get_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.result().found());
    EXPECT_EQ(response.result().job().job_id(), job_id);
    EXPECT_EQ(response.result().job().title(), "E2E Test Job - Get");
}

TEST_F(SchedulerE2ETest, GetJobNotFound) {
    proto::get_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    request.set_job_id("nonexistent-job-id-12345");

    proto::get_job_response response;
    grpc::ClientContext context;

    auto status = get_job_stub_->get_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_FALSE(response.result().found());
}

TEST_F(SchedulerE2ETest, ListJobs) {
    // Create a couple of jobs
    std::string job1 = CreateTestJob("E2E Test Job - List 1");
    std::string job2 = CreateTestJob("E2E Test Job - List 2");
    ASSERT_FALSE(job1.empty());
    ASSERT_FALSE(job2.empty());

    proto::list_jobs_request request;
    auto* filter = request.mutable_filter();
    filter->set_vehicle_id_filter(VEHICLE_ID);

    proto::list_jobs_response response;
    grpc::ClientContext context;

    auto status = list_jobs_stub_->list_jobs(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_GE(response.result().jobs_size(), 2);
}

TEST_F(SchedulerE2ETest, ListJobsEmpty) {
    proto::list_jobs_request request;
    auto* filter = request.mutable_filter();
    filter->set_vehicle_id_filter("NONEXISTENT_VEHICLE_12345");

    proto::list_jobs_response response;
    grpc::ClientContext context;

    auto status = list_jobs_stub_->list_jobs(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_EQ(response.result().jobs_size(), 0);
}

// =============================================================================
// Fleet Job Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CreateFleetJob) {
    proto::create_fleet_job_request request;
    auto* inner = request.mutable_request();
    inner->add_vehicle_ids(VEHICLE_ID);
    inner->set_title("E2E Fleet Job");
    inner->set_service("echo_service");
    inner->set_method("echo");
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-01-15T12:00:00Z"));

    proto::create_fleet_job_response response;
    grpc::ClientContext context;

    auto status = create_fleet_job_stub_->create_fleet_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_EQ(response.result().total_vehicles(), 1);
    EXPECT_GE(response.result().successful(), 1);

    // Track created jobs for cleanup
    for (const auto& result : response.result().results()) {
        if (result.success() && !result.job_id().empty()) {
            created_job_ids_.push_back(result.job_id());
        }
    }
}

// =============================================================================
// Validation Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CreateJobMissingVehicleId) {
    proto::create_job_request request;
    auto* inner = request.mutable_request();
    // Missing vehicle_id
    inner->set_title("Test Job");
    inner->set_service("echo_service");
    inner->set_method("echo");
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-01-05T12:00:00Z"));

    proto::create_job_response response;
    grpc::ClientContext context;

    auto status = create_job_stub_->create_job(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT);
}

TEST_F(SchedulerE2ETest, CreateJobMissingService) {
    proto::create_job_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_title("Test Job");
    // Missing service
    inner->set_method("echo");
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-01-05T12:00:00Z"));

    proto::create_job_response response;
    grpc::ClientContext context;

    auto status = create_job_stub_->create_job(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT);
}

TEST_F(SchedulerE2ETest, CreateJobMissingMethod) {
    proto::create_job_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_title("Test Job");
    inner->set_service("echo_service");
    // Missing method
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-01-05T12:00:00Z"));

    proto::create_job_response response;
    grpc::ClientContext context;

    auto status = create_job_stub_->create_job(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT);
}

TEST_F(SchedulerE2ETest, DeleteJobMissingJobId) {
    proto::delete_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    // Missing job_id

    proto::delete_job_response response;
    grpc::ClientContext context;

    auto status = delete_job_stub_->delete_job(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT);
}

TEST_F(SchedulerE2ETest, DeleteJobNotFound) {
    proto::delete_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    request.set_job_id("nonexistent-job-id-67890");

    proto::delete_job_response response;
    grpc::ClientContext context;

    auto status = delete_job_stub_->delete_job(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::NOT_FOUND);
}

TEST_F(SchedulerE2ETest, UpdateJobScheduledTime) {
    std::string job_id = CreateTestJob("E2E Test Job - Update Time");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    proto::update_job_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_job_id(job_id);
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-06-15T14:30:00Z"));

    proto::update_job_response response;
    grpc::ClientContext context;

    auto status = update_job_stub_->update_job(&context, request, &response);
    ASSERT_TRUE(status.ok()) << "gRPC call failed: " << status.error_message();
    EXPECT_TRUE(response.result().success()) << "Error: " << response.result().error_message();
}

TEST_F(SchedulerE2ETest, UpdateJobNotFound) {
    proto::update_job_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_job_id("nonexistent-job-id-update");
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-01-01T00:00:00Z"));

    proto::update_job_response response;
    grpc::ClientContext context;

    auto status = update_job_stub_->update_job(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::NOT_FOUND);
}

TEST_F(SchedulerE2ETest, UpdateJobMissingJobId) {
    proto::update_job_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    // Missing job_id
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-01-01T00:00:00Z"));

    proto::update_job_response response;
    grpc::ClientContext context;

    auto status = update_job_stub_->update_job(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT);
}

// =============================================================================
// Sync State Flow Tests (Cloud → Vehicle → Cloud)
// =============================================================================

TEST_F(SchedulerE2ETest, CreateJob_SyncStateFlow) {
    // Create job - should start as 'pending'
    proto::create_job_request request;
    auto* inner = request.mutable_request();
    inner->set_vehicle_id(VEHICLE_ID);
    inner->set_title("E2E Sync Test - Create");
    inner->set_service("echo_service");
    inner->set_method("echo");
    inner->set_scheduled_time_ms(Iso8601ToMs("2099-02-01T10:00:00Z"));

    proto::create_job_response response;
    grpc::ClientContext context;

    auto status = create_job_stub_->create_job(&context, request, &response);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(response.result().success());

    std::string job_id = response.result().job_id();
    created_job_ids_.push_back(job_id);

    // Wait for sync state to become 'synced' (vehicle confirmed)
    bool synced = WaitForSyncState(job_id, "synced", 15s);
    EXPECT_TRUE(synced) << "Job did not sync to vehicle within timeout";
}

TEST_F(SchedulerE2ETest, UpdateJob_SyncStateFlow) {
    // Create and wait for initial sync
    std::string job_id = CreateTestJob("E2E Sync Test - Update");
    ASSERT_FALSE(job_id.empty());

    bool initial_sync = WaitForSyncState(job_id, "synced", 15s);
    ASSERT_TRUE(initial_sync) << "Initial job sync failed";

    // Update job - should go back to 'pending'
    {
        proto::update_job_request request;
        auto* inner = request.mutable_request();
        inner->set_vehicle_id(VEHICLE_ID);
        inner->set_job_id(job_id);
        inner->set_scheduled_time_ms(Iso8601ToMs("2099-03-15T10:00:00Z"));

        proto::update_job_response response;
        grpc::ClientContext context;
        auto status = update_job_stub_->update_job(&context, request, &response);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(response.result().success());
    }

    // Wait for sync again
    bool update_sync = WaitForSyncState(job_id, "synced", 15s);
    EXPECT_TRUE(update_sync) << "Updated job did not re-sync within timeout";
}

TEST_F(SchedulerE2ETest, DeleteJob_SyncStateFlow) {
    // Create and wait for sync
    std::string job_id = CreateTestJob("E2E Sync Test - Delete");
    ASSERT_FALSE(job_id.empty());

    bool initial_sync = WaitForSyncState(job_id, "synced", 15s);
    ASSERT_TRUE(initial_sync) << "Initial job sync failed";

    // Delete job
    proto::delete_job_request request;
    request.set_vehicle_id(VEHICLE_ID);
    request.set_job_id(job_id);

    proto::delete_job_response response;
    grpc::ClientContext context;

    auto status = delete_job_stub_->delete_job(&context, request, &response);
    ASSERT_TRUE(status.ok());
    EXPECT_TRUE(response.result().success());

    // Remove from cleanup
    created_job_ids_.erase(
        std::remove(created_job_ids_.begin(), created_job_ids_.end(), job_id),
        created_job_ids_.end());

    // Wait for vehicle to sync the delete (job remains as tombstone)
    bool delete_synced = WaitForSyncState(job_id, "synced", 15s);
    EXPECT_TRUE(delete_synced) << "Delete sync did not complete";

    // Verify job still exists in DB as tombstone (deleted + synced)
    // Will be purged after retention period (e.g., 30 days) - not tested here
    {
        proto::get_job_request get_req;
        get_req.set_vehicle_id(VEHICLE_ID);
        get_req.set_job_id(job_id);

        proto::get_job_response get_resp;
        grpc::ClientContext ctx;
        get_job_stub_->get_job(&ctx, get_req, &get_resp);
        ASSERT_TRUE(get_resp.result().found()) << "Tombstone job should remain in DB";
        EXPECT_EQ(get_resp.result().job().sync_state(), proto::SYNC_CONFIRMED);
    }
}

// =============================================================================
// Full Workflow Test
// =============================================================================

TEST_F(SchedulerE2ETest, FullWorkflow_CreateAndReschedule) {
    const std::string INITIAL_SCHEDULE = "2099-04-01T09:00:00Z";
    const std::string NEW_SCHEDULE = "2099-04-15T14:00:00Z";

    // 1. Create job
    std::string job_id;
    {
        proto::create_job_request request;
        auto* inner = request.mutable_request();
        inner->set_vehicle_id(VEHICLE_ID);
        inner->set_title("E2E Full Workflow Job");
        inner->set_service("echo_service");
        inner->set_method("echo");
        inner->set_parameters_json(R"({"message": "hello"})");
        inner->set_scheduled_time_ms(Iso8601ToMs(INITIAL_SCHEDULE));

        proto::create_job_response response;
        grpc::ClientContext context;
        auto status = create_job_stub_->create_job(&context, request, &response);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(response.result().success());
        job_id = response.result().job_id();
        created_job_ids_.push_back(job_id);
    }

    // 2. Wait for initial sync
    ASSERT_TRUE(WaitForSyncState(job_id, "synced", 15s));

    // 3. Verify job details
    {
        proto::get_job_request request;
        request.set_vehicle_id(VEHICLE_ID);
        request.set_job_id(job_id);

        proto::get_job_response response;
        grpc::ClientContext context;
        auto status = get_job_stub_->get_job(&context, request, &response);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(response.result().found());
        EXPECT_EQ(response.result().job().title(), "E2E Full Workflow Job");
        EXPECT_EQ(response.result().job().service(), "echo_service");
    }

    // 4. Reschedule job
    {
        proto::update_job_request request;
        auto* inner = request.mutable_request();
        inner->set_vehicle_id(VEHICLE_ID);
        inner->set_job_id(job_id);
        inner->set_scheduled_time_ms(Iso8601ToMs(NEW_SCHEDULE));

        proto::update_job_response response;
        grpc::ClientContext context;
        auto status = update_job_stub_->update_job(&context, request, &response);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(response.result().success());
    }

    // 5. Wait for re-sync
    ASSERT_TRUE(WaitForSyncState(job_id, "synced", 15s));

    // 6. Verify updated schedule
    {
        proto::get_job_request request;
        request.set_vehicle_id(VEHICLE_ID);
        request.set_job_id(job_id);

        proto::get_job_response response;
        grpc::ClientContext context;
        auto status = get_job_stub_->get_job(&context, request, &response);
        ASSERT_TRUE(status.ok());
        ASSERT_TRUE(response.result().found());
        EXPECT_EQ(response.result().job().scheduled_time_ms(), Iso8601ToMs(NEW_SCHEDULE));
    }

    // 7. Pause job
    {
        proto::pause_job_request request;
        request.set_vehicle_id(VEHICLE_ID);
        request.set_job_id(job_id);

        proto::pause_job_response response;
        grpc::ClientContext context;
        auto status = pause_job_stub_->pause_job(&context, request, &response);
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(response.result().success());
    }

    // 8. Resume job
    {
        proto::resume_job_request request;
        request.set_vehicle_id(VEHICLE_ID);
        request.set_job_id(job_id);

        proto::resume_job_response response;
        grpc::ClientContext context;
        auto status = resume_job_stub_->resume_job(&context, request, &response);
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(response.result().success());
    }

    // 9. Delete job
    {
        proto::delete_job_request request;
        request.set_vehicle_id(VEHICLE_ID);
        request.set_job_id(job_id);

        proto::delete_job_response response;
        grpc::ClientContext context;
        auto status = delete_job_stub_->delete_job(&context, request, &response);
        ASSERT_TRUE(status.ok());
        EXPECT_TRUE(response.result().success());

        created_job_ids_.erase(
            std::remove(created_job_ids_.begin(), created_job_ids_.end(), job_id),
            created_job_ids_.end());
    }

    LOG(INFO) << "Full workflow test completed successfully";
}
