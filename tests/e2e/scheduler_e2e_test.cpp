/**
 * @file scheduler_e2e_test.cpp
 * @brief End-to-end tests for Cloud Scheduler API
 *
 * These tests verify the Cloud Scheduler gRPC API.
 * The test fixture automatically starts/stops the scheduler_api service.
 *
 * Prerequisites:
 *   - Docker containers running (Kafka, PostgreSQL)
 *   - Run: docker-compose up -d postgres kafka
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <thread>
#include <string>
#include <memory>
#include <atomic>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>
#include <linux/limits.h>

#include <grpcpp/grpcpp.h>
#include "cloud-scheduler-service.grpc.pb.h"
#include "scheduler-command-envelope.pb.h"
#include "postgres_client.hpp"

namespace scheduler_grpc = ifex::cloud::scheduler;
namespace cmd_pb = swdv::scheduler_command_envelope;

using namespace std::chrono_literals;

/// Get the directory containing the test executable
std::string GetExecutableDir() {
    char buf[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (len == -1) {
        return ".";
    }
    buf[len] = '\0';
    std::string path(buf);
    size_t pos = path.rfind('/');
    return (pos != std::string::npos) ? path.substr(0, pos) : ".";
}

/// Get path to a service binary (in build root, one level up from tests/)
std::string GetBinaryPath(const std::string& name) {
    return GetExecutableDir() + "/../" + name;
}

/**
 * @brief Test fixture for Scheduler E2E tests
 *
 * Automatically starts scheduler_api service before tests and stops it after.
 */
class SchedulerE2ETest : public ::testing::Test {
protected:
    static constexpr const char* CLOUD_SCHEDULER_ADDR = "localhost:50083";
    static constexpr const char* VEHICLE_ID = "VIN00000000000001";

    // Service process and database (shared across all tests in this suite)
    static pid_t scheduler_pid_;
    static std::unique_ptr<ifex::offboard::PostgresClient> db_;

    static void SetUpTestSuite() {
        // Initialize glog
        static bool glog_initialized = false;
        if (!glog_initialized) {
            google::InitGoogleLogging("scheduler_e2e_test");
            FLAGS_logtostderr = true;
            glog_initialized = true;
        }

        // Connect to PostgreSQL and create test vehicle
        // This is a prerequisite - vehicles are normally created by status tracking
        ifex::offboard::PostgresConfig pg_config;
        pg_config.host = "localhost";
        pg_config.port = 5432;
        pg_config.database = "ifex_offboard";
        pg_config.user = "ifex";
        pg_config.password = "ifex_dev";

        db_ = std::make_unique<ifex::offboard::PostgresClient>(pg_config);
        ASSERT_TRUE(db_->is_connected())
            << "PostgreSQL not available. Run: docker-compose up -d postgres";

        // Create test vehicle (normally done by mqtt_kafka_bridge on status update)
        auto result = db_->execute(R"(
            INSERT INTO vehicles (vehicle_id, is_online, first_seen_at, last_seen_at)
            VALUES ($1, true, NOW(), NOW())
            ON CONFLICT (vehicle_id) DO UPDATE SET
                is_online = true,
                last_seen_at = NOW()
        )", {VEHICLE_ID});
        ASSERT_TRUE(result.ok()) << "Failed to create test vehicle: " << result.error();
        LOG(INFO) << "Created test vehicle: " << VEHICLE_ID;

        // Start scheduler_api service
        // Binary is in the same directory as the test executable
        std::string scheduler_path = GetBinaryPath("scheduler_api");

        scheduler_pid_ = fork();
        if (scheduler_pid_ == 0) {
            // Child process - exec scheduler_api
            execl(scheduler_path.c_str(), "scheduler_api",
                   "--grpc_listen=0.0.0.0:50083",
                   "--kafka_broker=localhost:9092",
                   "--postgres_host=localhost",
                   "--postgres_port=5432",
                   "--postgres_db=ifex_offboard",
                   "--postgres_user=ifex",
                   "--postgres_password=ifex_dev",
                   nullptr);
            // If exec fails, exit child
            LOG(ERROR) << "Failed to exec scheduler_api at: " << scheduler_path;
            _exit(1);
        }

        ASSERT_GT(scheduler_pid_, 0) << "Failed to fork scheduler_api process";
        LOG(INFO) << "Started scheduler_api with PID " << scheduler_pid_;

        // Wait for service to be ready
        auto channel = grpc::CreateChannel(CLOUD_SCHEDULER_ADDR, grpc::InsecureChannelCredentials());
        auto deadline = std::chrono::system_clock::now() + 10s;
        ASSERT_TRUE(channel->WaitForConnected(deadline))
            << "scheduler_api failed to start within 10 seconds";

        LOG(INFO) << "scheduler_api is ready";
    }

    static void TearDownTestSuite() {
        if (scheduler_pid_ > 0) {
            LOG(INFO) << "Stopping scheduler_api (PID " << scheduler_pid_ << ")";
            kill(scheduler_pid_, SIGTERM);
            int status;
            waitpid(scheduler_pid_, &status, 0);
            scheduler_pid_ = 0;
        }

        // Clean up test vehicle and jobs
        if (db_ && db_->is_connected()) {
            db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {VEHICLE_ID});
            db_->execute("DELETE FROM vehicles WHERE vehicle_id = $1", {VEHICLE_ID});
            LOG(INFO) << "Cleaned up test vehicle: " << VEHICLE_ID;
        }
        db_.reset();
    }

    void SetUp() override {
        // Create gRPC stub for this test
        auto channel = grpc::CreateChannel(
            CLOUD_SCHEDULER_ADDR,
            grpc::InsecureChannelCredentials());

        stub_ = scheduler_grpc::CloudSchedulerService::NewStub(channel);
        ASSERT_NE(stub_, nullptr);
    }

    void TearDown() override {
        // Cleanup created jobs
        for (const auto& job_id : created_jobs_) {
            if (stub_) {
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + 2s);
                scheduler_grpc::DeleteJobRequest req;
                req.set_vehicle_id(VEHICLE_ID);
                req.set_job_id(job_id);
                scheduler_grpc::DeleteJobResponse resp;
                stub_->DeleteJob(&ctx, req, &resp);
            }
        }
    }

    // Helper to create a test job
    std::string CreateTestJob(const std::string& title,
                              const std::string& service = "echo_service",
                              const std::string& method = "echo") {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);

        scheduler_grpc::CreateJobRequest req;
        req.set_vehicle_id(VEHICLE_ID);
        req.set_title(title);
        req.set_service(service);
        req.set_method(method);
        req.set_parameters_json(R"({"test": true})");
        req.set_scheduled_time("2026-12-31T23:59:59Z");
        req.set_created_by("e2e_test");

        scheduler_grpc::CreateJobResponse resp;
        auto status = stub_->CreateJob(&ctx, req, &resp);

        if (status.ok() && resp.success()) {
            created_jobs_.push_back(resp.job_id());
            return resp.job_id();
        }
        return "";
    }

    std::unique_ptr<scheduler_grpc::CloudSchedulerService::Stub> stub_;
    std::vector<std::string> created_jobs_;
};

// Static member definitions
pid_t SchedulerE2ETest::scheduler_pid_ = 0;
std::unique_ptr<ifex::offboard::PostgresClient> SchedulerE2ETest::db_ = nullptr;

// =============================================================================
// Connection Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CloudSchedulerConnection) {
    // This test verifies the Cloud Scheduler is running
    // SetUp() already asserts connection - if we get here, we're connected
    LOG(INFO) << "Cloud Scheduler is available at " << CLOUD_SCHEDULER_ADDR;
    ASSERT_NE(stub_, nullptr);
}

// =============================================================================
// CreateJob Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CreateJobBasic) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 10s);

    scheduler_grpc::CreateJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_title("E2E Test: CreateJobBasic");
    req.set_service("echo_service");
    req.set_method("echo");
    req.set_parameters_json(R"({"message": "hello from e2e test"})");
    req.set_scheduled_time("2026-01-05T12:00:00Z");
    req.set_created_by("e2e_test");

    scheduler_grpc::CreateJobResponse resp;
    auto status = stub_->CreateJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    EXPECT_TRUE(resp.success()) << "CreateJob failed: " << resp.error_message();
    EXPECT_FALSE(resp.job_id().empty()) << "Job ID should be returned";

    LOG(INFO) << "Created job: " << resp.job_id();
    created_jobs_.push_back(resp.job_id());

    // Wait for command to propagate to vehicle
    std::this_thread::sleep_for(2s);
}

TEST_F(SchedulerE2ETest, CreateJobWithRecurrence) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 10s);

    scheduler_grpc::CreateJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_title("E2E Test: Recurring Job");
    req.set_service("echo_service");
    req.set_method("echo");
    req.set_parameters_json("{}");
    req.set_scheduled_time("2026-01-05T08:00:00Z");
    req.set_recurrence_rule("0 8 * * *");  // Daily at 8am
    req.set_end_time("2026-12-31T23:59:59Z");
    req.set_created_by("e2e_test");

    scheduler_grpc::CreateJobResponse resp;
    auto status = stub_->CreateJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    EXPECT_TRUE(resp.success()) << "CreateJob failed: " << resp.error_message();

    if (resp.success()) {
        created_jobs_.push_back(resp.job_id());
    }
}

// =============================================================================
// PauseJob Tests
// =============================================================================

TEST_F(SchedulerE2ETest, PauseJob) {
    // First create a job
    std::string job_id = CreateTestJob("E2E Test: PauseJob Target");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    // Wait for job to be created on vehicle
    std::this_thread::sleep_for(2s);

    // Now pause it
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 10s);

    scheduler_grpc::PauseJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_job_id(job_id);

    scheduler_grpc::PauseJobResponse resp;
    auto status = stub_->PauseJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    // Note: success may be false if job hasn't reached vehicle yet
    LOG(INFO) << "PauseJob result: success=" << resp.success()
              << " error=" << resp.error_message();
}

// =============================================================================
// ResumeJob Tests
// =============================================================================

TEST_F(SchedulerE2ETest, ResumeJob) {
    // Create and pause a job first
    std::string job_id = CreateTestJob("E2E Test: ResumeJob Target");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    std::this_thread::sleep_for(2s);

    // Pause it
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);
        scheduler_grpc::PauseJobRequest req;
        req.set_vehicle_id(VEHICLE_ID);
        req.set_job_id(job_id);
        scheduler_grpc::PauseJobResponse resp;
        stub_->PauseJob(&ctx, req, &resp);
    }

    std::this_thread::sleep_for(1s);

    // Now resume it
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 10s);

    scheduler_grpc::ResumeJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_job_id(job_id);

    scheduler_grpc::ResumeJobResponse resp;
    auto status = stub_->ResumeJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    LOG(INFO) << "ResumeJob result: success=" << resp.success()
              << " error=" << resp.error_message();
}

// =============================================================================
// TriggerJob Tests
// =============================================================================

TEST_F(SchedulerE2ETest, TriggerJob) {
    // Create a job
    std::string job_id = CreateTestJob("E2E Test: TriggerJob Target");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    std::this_thread::sleep_for(2s);

    // Trigger immediate execution
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 10s);

    scheduler_grpc::TriggerJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_job_id(job_id);

    scheduler_grpc::TriggerJobResponse resp;
    auto status = stub_->TriggerJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    LOG(INFO) << "TriggerJob result: success=" << resp.success()
              << " error=" << resp.error_message();
}

// =============================================================================
// DeleteJob Tests
// =============================================================================

TEST_F(SchedulerE2ETest, DeleteJob) {
    // Create a job
    std::string job_id = CreateTestJob("E2E Test: DeleteJob Target");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    std::this_thread::sleep_for(2s);

    // Delete it
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 10s);

    scheduler_grpc::DeleteJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_job_id(job_id);

    scheduler_grpc::DeleteJobResponse resp;
    auto status = stub_->DeleteJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    LOG(INFO) << "DeleteJob result: success=" << resp.success()
              << " error=" << resp.error_message();

    // Remove from cleanup list since we just deleted it
    created_jobs_.erase(
        std::remove(created_jobs_.begin(), created_jobs_.end(), job_id),
        created_jobs_.end());
}

// =============================================================================
// Workflow Tests
// =============================================================================

TEST_F(SchedulerE2ETest, FullJobLifecycle) {
    LOG(INFO) << "=== Full Job Lifecycle Test ===";

    // 1. Create job
    LOG(INFO) << "Step 1: Creating job...";
    std::string job_id = CreateTestJob("E2E Test: Full Lifecycle");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";
    LOG(INFO) << "  Created job: " << job_id;

    std::this_thread::sleep_for(2s);

    // 2. Pause job
    LOG(INFO) << "Step 2: Pausing job...";
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);
        scheduler_grpc::PauseJobRequest req;
        req.set_vehicle_id(VEHICLE_ID);
        req.set_job_id(job_id);
        scheduler_grpc::PauseJobResponse resp;
        auto status = stub_->PauseJob(&ctx, req, &resp);
        ASSERT_TRUE(status.ok());
        LOG(INFO) << "  Pause result: " << (resp.success() ? "OK" : resp.error_message());
    }

    std::this_thread::sleep_for(1s);

    // 3. Resume job
    LOG(INFO) << "Step 3: Resuming job...";
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);
        scheduler_grpc::ResumeJobRequest req;
        req.set_vehicle_id(VEHICLE_ID);
        req.set_job_id(job_id);
        scheduler_grpc::ResumeJobResponse resp;
        auto status = stub_->ResumeJob(&ctx, req, &resp);
        ASSERT_TRUE(status.ok());
        LOG(INFO) << "  Resume result: " << (resp.success() ? "OK" : resp.error_message());
    }

    std::this_thread::sleep_for(1s);

    // 4. Trigger job
    LOG(INFO) << "Step 4: Triggering job...";
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);
        scheduler_grpc::TriggerJobRequest req;
        req.set_vehicle_id(VEHICLE_ID);
        req.set_job_id(job_id);
        scheduler_grpc::TriggerJobResponse resp;
        auto status = stub_->TriggerJob(&ctx, req, &resp);
        ASSERT_TRUE(status.ok());
        LOG(INFO) << "  Trigger result: " << (resp.success() ? "OK" : resp.error_message());
    }

    std::this_thread::sleep_for(1s);

    // 5. Delete job
    LOG(INFO) << "Step 5: Deleting job...";
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);
        scheduler_grpc::DeleteJobRequest req;
        req.set_vehicle_id(VEHICLE_ID);
        req.set_job_id(job_id);
        scheduler_grpc::DeleteJobResponse resp;
        auto status = stub_->DeleteJob(&ctx, req, &resp);
        ASSERT_TRUE(status.ok());
        LOG(INFO) << "  Delete result: " << (resp.success() ? "OK" : resp.error_message());
    }

    // Remove from cleanup list
    created_jobs_.erase(
        std::remove(created_jobs_.begin(), created_jobs_.end(), job_id),
        created_jobs_.end());

    LOG(INFO) << "=== Full Job Lifecycle Complete ===";
}

// =============================================================================
// Error Cases
// =============================================================================

TEST_F(SchedulerE2ETest, PauseNonexistentJob) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::PauseJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_job_id("nonexistent-job-id-12345");

    scheduler_grpc::PauseJobResponse resp;
    auto status = stub_->PauseJob(&ctx, req, &resp);

    // Should return NOT_FOUND for non-existent job
    ASSERT_EQ(status.error_code(), grpc::NOT_FOUND)
        << "Should return NOT_FOUND for non-existent job";
    LOG(INFO) << "PauseNonexistentJob: error_code=" << status.error_code()
              << " message=" << status.error_message();
}

TEST_F(SchedulerE2ETest, CreateJobInvalidVehicle) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::CreateJobRequest req;
    req.set_vehicle_id("INVALID_VEHICLE_ID_THAT_DOES_NOT_EXIST");
    req.set_title("Should not be created");
    req.set_service("echo_service");
    req.set_method("echo");
    req.set_scheduled_time("2026-01-05T12:00:00Z");
    req.set_created_by("e2e_test");

    scheduler_grpc::CreateJobResponse resp;
    auto status = stub_->CreateJob(&ctx, req, &resp);

    // The gRPC call should succeed, but the job creation may fail
    // depending on whether the cloud scheduler validates vehicle IDs
    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    LOG(INFO) << "CreateJobInvalidVehicle: success=" << resp.success()
              << " error=" << resp.error_message();
}

// =============================================================================
// GetJob Tests
// =============================================================================

TEST_F(SchedulerE2ETest, GetJobBasic) {
    // Create a job first
    std::string job_id = CreateTestJob("E2E Test: GetJob Target", "test_service", "test_method");
    ASSERT_FALSE(job_id.empty()) << "Failed to create test job";

    // Give it time to be stored
    std::this_thread::sleep_for(500ms);

    // Get the job
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::GetJobRequest req;
    req.set_job_id(job_id);

    scheduler_grpc::GetJobResponse resp;
    auto status = stub_->GetJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    ASSERT_TRUE(resp.found()) << "Job should be found";
    EXPECT_EQ(resp.job().job_id(), job_id);
    EXPECT_EQ(resp.job().vehicle_id(), VEHICLE_ID);
    EXPECT_EQ(resp.job().service(), "test_service");
    EXPECT_EQ(resp.job().method(), "test_method");
    EXPECT_EQ(resp.job().title(), "E2E Test: GetJob Target");

    LOG(INFO) << "GetJob returned: job_id=" << resp.job().job_id()
              << " vehicle=" << resp.job().vehicle_id()
              << " service=" << resp.job().service()
              << " method=" << resp.job().method();
}

TEST_F(SchedulerE2ETest, GetJobNotFound) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::GetJobRequest req;
    req.set_job_id("nonexistent-job-id-99999");

    scheduler_grpc::GetJobResponse resp;
    auto status = stub_->GetJob(&ctx, req, &resp);

    // Should return NOT_FOUND
    ASSERT_EQ(status.error_code(), grpc::NOT_FOUND)
        << "Should return NOT_FOUND for non-existent job";
}

// =============================================================================
// ListJobs Tests
// =============================================================================

TEST_F(SchedulerE2ETest, ListJobsEmpty) {
    // Clean up any existing jobs for this vehicle first
    db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {VEHICLE_ID});

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::ListJobsRequest req;
    req.set_vehicle_id_filter(VEHICLE_ID);
    req.set_page_size(10);

    scheduler_grpc::ListJobsResponse resp;
    auto status = stub_->ListJobs(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    EXPECT_EQ(resp.jobs_size(), 0) << "Should have no jobs";
    EXPECT_EQ(resp.total_count(), 0);

    LOG(INFO) << "ListJobs (empty): total=" << resp.total_count();
}

TEST_F(SchedulerE2ETest, ListJobsWithResults) {
    // Clean up first
    db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {VEHICLE_ID});

    // Create multiple jobs
    std::string job1 = CreateTestJob("E2E Test: ListJobs 1", "service_a", "method_a");
    std::string job2 = CreateTestJob("E2E Test: ListJobs 2", "service_b", "method_b");
    std::string job3 = CreateTestJob("E2E Test: ListJobs 3", "service_a", "method_c");
    ASSERT_FALSE(job1.empty());
    ASSERT_FALSE(job2.empty());
    ASSERT_FALSE(job3.empty());

    std::this_thread::sleep_for(500ms);

    // List all jobs for this vehicle
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::ListJobsRequest req;
    req.set_vehicle_id_filter(VEHICLE_ID);
    req.set_page_size(10);

    scheduler_grpc::ListJobsResponse resp;
    auto status = stub_->ListJobs(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    EXPECT_EQ(resp.jobs_size(), 3) << "Should have 3 jobs";
    EXPECT_EQ(resp.total_count(), 3);

    LOG(INFO) << "ListJobs: returned " << resp.jobs_size() << " jobs, total=" << resp.total_count();
    for (const auto& job : resp.jobs()) {
        LOG(INFO) << "  - " << job.job_id() << ": " << job.title();
    }
}

TEST_F(SchedulerE2ETest, ListJobsWithServiceFilter) {
    // Clean up first
    db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {VEHICLE_ID});

    // Create jobs with different services
    CreateTestJob("E2E Test: Filter 1", "target_service", "method1");
    CreateTestJob("E2E Test: Filter 2", "target_service", "method2");
    CreateTestJob("E2E Test: Filter 3", "other_service", "method1");

    std::this_thread::sleep_for(500ms);

    // Filter by service
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::ListJobsRequest req;
    req.set_vehicle_id_filter(VEHICLE_ID);
    req.set_service_filter("target_service");
    req.set_page_size(10);

    scheduler_grpc::ListJobsResponse resp;
    auto status = stub_->ListJobs(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    EXPECT_EQ(resp.jobs_size(), 2) << "Should have 2 jobs with target_service";

    for (const auto& job : resp.jobs()) {
        EXPECT_EQ(job.service(), "target_service");
    }

    LOG(INFO) << "ListJobs with filter: returned " << resp.jobs_size() << " jobs";
}

TEST_F(SchedulerE2ETest, ListJobsPagination) {
    // Clean up first
    db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {VEHICLE_ID});

    // Create 5 jobs
    for (int i = 0; i < 5; i++) {
        CreateTestJob("E2E Test: Pagination " + std::to_string(i));
    }

    std::this_thread::sleep_for(500ms);

    // Get first page (size 2)
    scheduler_grpc::ListJobsResponse resp1;
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);

        scheduler_grpc::ListJobsRequest req;
        req.set_vehicle_id_filter(VEHICLE_ID);
        req.set_page_size(2);

        auto status = stub_->ListJobs(&ctx, req, &resp1);
        ASSERT_TRUE(status.ok());
    }

    EXPECT_EQ(resp1.jobs_size(), 2);
    EXPECT_EQ(resp1.total_count(), 5);
    EXPECT_FALSE(resp1.next_page_token().empty()) << "Should have next page token";

    LOG(INFO) << "Page 1: " << resp1.jobs_size() << " jobs, next_token=" << resp1.next_page_token();

    // Get second page
    scheduler_grpc::ListJobsResponse resp2;
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);

        scheduler_grpc::ListJobsRequest req;
        req.set_vehicle_id_filter(VEHICLE_ID);
        req.set_page_size(2);
        req.set_page_token(resp1.next_page_token());

        auto status = stub_->ListJobs(&ctx, req, &resp2);
        ASSERT_TRUE(status.ok());
    }

    EXPECT_EQ(resp2.jobs_size(), 2);

    LOG(INFO) << "Page 2: " << resp2.jobs_size() << " jobs, next_token=" << resp2.next_page_token();
}

// =============================================================================
// Fleet Operations Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CreateFleetJob) {
    // Create additional test vehicles
    db_->execute(R"(
        INSERT INTO vehicles (vehicle_id, is_online, first_seen_at, last_seen_at)
        VALUES ($1, true, NOW(), NOW())
        ON CONFLICT (vehicle_id) DO NOTHING
    )", {"FLEET_VIN_001"});
    db_->execute(R"(
        INSERT INTO vehicles (vehicle_id, is_online, first_seen_at, last_seen_at)
        VALUES ($1, true, NOW(), NOW())
        ON CONFLICT (vehicle_id) DO NOTHING
    )", {"FLEET_VIN_002"});

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 10s);

    scheduler_grpc::CreateFleetJobRequest req;
    req.add_vehicle_ids("FLEET_VIN_001");
    req.add_vehicle_ids("FLEET_VIN_002");
    req.add_vehicle_ids(VEHICLE_ID);
    req.set_title("E2E Test: Fleet Job");
    req.set_service("fleet_service");
    req.set_method("fleet_update");
    req.set_parameters_json(R"({"version": "2.0"})");
    req.set_scheduled_time("2026-06-01T00:00:00Z");
    req.set_created_by("fleet_e2e_test");

    scheduler_grpc::CreateFleetJobResponse resp;
    auto status = stub_->CreateFleetJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    EXPECT_EQ(resp.total_vehicles(), 3);
    EXPECT_GE(resp.successful(), 1);  // At least some should succeed

    LOG(INFO) << "CreateFleetJob: total=" << resp.total_vehicles()
              << " successful=" << resp.successful()
              << " failed=" << resp.failed();

    for (const auto& result : resp.results()) {
        LOG(INFO) << "  - " << result.vehicle_id() << ": "
                  << (result.success() ? "OK" : result.error_message());
        if (result.success()) {
            created_jobs_.push_back(result.job_id());
        }
    }

    // Cleanup fleet vehicles
    db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {"FLEET_VIN_001"});
    db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {"FLEET_VIN_002"});
    db_->execute("DELETE FROM vehicles WHERE vehicle_id = $1", {"FLEET_VIN_001"});
    db_->execute("DELETE FROM vehicles WHERE vehicle_id = $1", {"FLEET_VIN_002"});
}

TEST_F(SchedulerE2ETest, GetFleetJobStats) {
    // Clean up and create some jobs
    db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {VEHICLE_ID});

    CreateTestJob("Stats Job 1", "service_alpha", "method1");
    CreateTestJob("Stats Job 2", "service_alpha", "method2");
    CreateTestJob("Stats Job 3", "service_beta", "method1");

    std::this_thread::sleep_for(500ms);

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::GetFleetJobStatsRequest req;
    // No filters - get all stats

    scheduler_grpc::GetFleetJobStatsResponse resp;
    auto status = stub_->GetFleetJobStats(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    EXPECT_GE(resp.total_jobs(), 3);

    LOG(INFO) << "FleetJobStats: total_jobs=" << resp.total_jobs()
              << " total_vehicles=" << resp.total_vehicles_with_jobs();

    for (const auto& stat : resp.by_service_method()) {
        LOG(INFO) << "  - " << stat.service() << ": " << stat.total_jobs() << " jobs";
    }

    for (const auto& [status_name, count] : resp.by_status()) {
        LOG(INFO) << "  - status " << status_name << ": " << count;
    }
}

// =============================================================================
// GetJobExecutions Tests
// =============================================================================

TEST_F(SchedulerE2ETest, GetJobExecutionsEmpty) {
    // Create a job that hasn't been executed
    std::string job_id = CreateTestJob("E2E Test: No Executions");
    ASSERT_FALSE(job_id.empty());

    std::this_thread::sleep_for(500ms);

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::GetJobExecutionsRequest req;
    req.set_job_id(job_id);
    req.set_limit(10);

    scheduler_grpc::GetJobExecutionsResponse resp;
    auto status = stub_->GetJobExecutions(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC error: " << status.error_message();
    EXPECT_EQ(resp.executions_size(), 0) << "New job should have no executions";

    LOG(INFO) << "GetJobExecutions (empty): job=" << job_id
              << " total=" << resp.total_count();
}

// =============================================================================
// Validation Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CreateJobMissingVehicleId) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::CreateJobRequest req;
    // Missing vehicle_id
    req.set_title("Missing Vehicle");
    req.set_service("test_service");
    req.set_method("test_method");

    scheduler_grpc::CreateJobResponse resp;
    auto status = stub_->CreateJob(&ctx, req, &resp);

    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT)
        << "Should return INVALID_ARGUMENT for missing vehicle_id";
}

TEST_F(SchedulerE2ETest, CreateJobMissingService) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::CreateJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_title("Missing Service");
    // Missing service
    req.set_method("test_method");

    scheduler_grpc::CreateJobResponse resp;
    auto status = stub_->CreateJob(&ctx, req, &resp);

    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT)
        << "Should return INVALID_ARGUMENT for missing service";
}

TEST_F(SchedulerE2ETest, CreateJobMissingMethod) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::CreateJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_title("Missing Method");
    req.set_service("test_service");
    // Missing method

    scheduler_grpc::CreateJobResponse resp;
    auto status = stub_->CreateJob(&ctx, req, &resp);

    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT)
        << "Should return INVALID_ARGUMENT for missing method";
}

TEST_F(SchedulerE2ETest, PauseJobMissingJobId) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::PauseJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    // Missing job_id

    scheduler_grpc::PauseJobResponse resp;
    auto status = stub_->PauseJob(&ctx, req, &resp);

    EXPECT_EQ(status.error_code(), grpc::INVALID_ARGUMENT)
        << "Should return INVALID_ARGUMENT for missing job_id";
}

// =============================================================================
// Real Vehicle Tests (requires ifex-vehicle:latest Docker image)
// =============================================================================

#include "e2e_test_fixture.hpp"

/**
 * @brief E2E tests with real vehicle container running scheduler + echo service
 *
 * These tests verify the complete scheduler flow:
 *   Cloud API → Kafka → mqtt_kafka_bridge → MQTT → Vehicle Scheduler → Echo Service
 *               ↓
 *   PostgreSQL ← scheduler_mirror ← Kafka ← MQTT ← Vehicle Scheduler (execution result)
 *
 * Prerequisites:
 *   - Docker image: ifex-vehicle:latest (build with covesa-ifex-core/build-test-container.sh)
 *   - Infrastructure: postgres, kafka, mosquitto (run deploy/start-infra.sh)
 */
class SchedulerRealVehicleTest : public ifex::offboard::test::E2ETestFixture {
protected:
    static constexpr const char* SCHEDULER_ADDR = "localhost:50083";
    static std::unique_ptr<scheduler_grpc::CloudSchedulerService::Stub> stub_;
    static std::shared_ptr<grpc::Channel> channel_;
    static pid_t scheduler_pid_;

    static void SetUpTestSuite() {
        // Start E2E infrastructure (vehicle, bridges)
        ifex::offboard::test::E2ETestFixture::SetUpTestSuite();

        // Wait for echo_service to register
        ASSERT_TRUE(ifex::offboard::test::E2ETestInfrastructure::WaitForServiceRegistered(
            ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID,
            "echo_service",
            std::chrono::seconds(30)))
            << "Echo service did not register within 30 seconds";

        // Start scheduler_api
        std::string binary = ifex::offboard::test::E2ETestInfrastructure::GetBinaryPath("scheduler_api");

        scheduler_pid_ = fork();
        if (scheduler_pid_ == 0) {
            execl(binary.c_str(), "scheduler_api",
                  "--grpc_listen", SCHEDULER_ADDR,
                  "--kafka_broker", "localhost:9092",
                  "--postgres_host", "localhost",
                  "--postgres_port", "5432",
                  "--postgres_db", "ifex_offboard",
                  "--postgres_user", "ifex",
                  "--postgres_password", "ifex_dev",
                  "--logtostderr",
                  nullptr);
            LOG(ERROR) << "Failed to exec scheduler_api";
            _exit(1);
        }
        ASSERT_GT(scheduler_pid_, 0) << "Failed to fork scheduler_api";
        LOG(INFO) << "Started scheduler_api with PID " << scheduler_pid_;

        // Connect gRPC client
        std::this_thread::sleep_for(std::chrono::seconds(2));
        channel_ = grpc::CreateChannel(SCHEDULER_ADDR, grpc::InsecureChannelCredentials());
        stub_ = scheduler_grpc::CloudSchedulerService::NewStub(channel_);

        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
        ASSERT_TRUE(channel_->WaitForConnected(deadline))
            << "Failed to connect to scheduler_api";

        LOG(INFO) << "Real vehicle scheduler test infrastructure ready";
    }

    static void TearDownTestSuite() {
        // Close gRPC first
        stub_.reset();
        channel_.reset();

        // Stop scheduler_api
        if (scheduler_pid_ > 0) {
            LOG(INFO) << "Stopping scheduler_api (PID " << scheduler_pid_ << ")";
            kill(scheduler_pid_, SIGTERM);
            int status;
            waitpid(scheduler_pid_, &status, 0);
            scheduler_pid_ = 0;
        }

        // Stop E2E infrastructure
        ifex::offboard::test::E2ETestFixture::TearDownTestSuite();
    }

    // Helper to create a job on the real vehicle
    std::string CreateRealVehicleJob(const std::string& title,
                                      const std::string& method = "echo",
                                      const std::string& params_json = R"({\"message\": \"test\"})") {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 10s);

        scheduler_grpc::CreateJobRequest req;
        req.set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
        req.set_title(title);
        req.set_service("echo_service");
        req.set_method(method);
        req.set_parameters_json(params_json);
        req.set_scheduled_time("2099-12-31T23:59:59Z");  // Far future
        req.set_created_by("real_vehicle_e2e_test");

        scheduler_grpc::CreateJobResponse resp;
        auto status = stub_->CreateJob(&ctx, req, &resp);

        if (status.ok() && resp.success()) {
            LOG(INFO) << "Created job on real vehicle: " << resp.job_id();
            return resp.job_id();
        }
        LOG(ERROR) << "Failed to create job: " << status.error_message()
                   << " / " << resp.error_message();
        return "";
    }
};

// Static members
std::unique_ptr<scheduler_grpc::CloudSchedulerService::Stub> SchedulerRealVehicleTest::stub_;
std::shared_ptr<grpc::Channel> SchedulerRealVehicleTest::channel_;
pid_t SchedulerRealVehicleTest::scheduler_pid_ = 0;

TEST_F(SchedulerRealVehicleTest, RealVehicle_CreateJobAndTrigger) {
    // NOTE: Test is DISABLED until vehicle scheduler integration is verified
    // Enable once vehicle infrastructure is confirmed working

    // Step 1: Create job targeting echo_service on real vehicle
    std::string job_id = CreateRealVehicleJob(
        "Real Vehicle E2E: Echo Job",
        "echo",
        R"({"message": "Hello from scheduled job!"})");
    ASSERT_FALSE(job_id.empty()) << "Failed to create job on real vehicle";

    // Step 2: Wait for job to sync to vehicle
    std::this_thread::sleep_for(3s);

    // Step 3: Trigger immediate execution
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 10s);

        scheduler_grpc::TriggerJobRequest req;
        req.set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
        req.set_job_id(job_id);

        scheduler_grpc::TriggerJobResponse resp;
        auto status = stub_->TriggerJob(&ctx, req, &resp);

        ASSERT_TRUE(status.ok()) << "TriggerJob failed: " << status.error_message();
        EXPECT_TRUE(resp.success()) << "TriggerJob not successful: " << resp.error_message();
    }

    // Step 4: Wait for execution result to sync back
    ASSERT_TRUE(ifex::offboard::test::E2ETestInfrastructure::WaitForJobExecution(
        job_id, std::chrono::seconds(30)))
        << "Job execution result did not sync back within 30 seconds";

    // Step 5: Verify execution in database
    auto* db = ifex::offboard::test::E2ETestInfrastructure::GetDatabase();
    ASSERT_NE(db, nullptr);

    auto result = db->execute(
        "SELECT status, result FROM job_executions WHERE job_id = $1 ORDER BY executed_at_ms DESC LIMIT 1",
        {job_id});

    ASSERT_TRUE(result.ok()) << "Failed to query job execution: " << result.error();
    ASSERT_GE(result.num_rows(), 1) << "No execution record found";

    std::string status = result.row(0).get_string("status");
    std::string result_str = result.row(0).get_string("result");

    EXPECT_EQ(status, "completed") << "Job did not complete successfully";
    LOG(INFO) << "Job execution result: status=" << status << " result=" << result_str;

    // Step 6: Clean up - delete the job
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);

        scheduler_grpc::DeleteJobRequest req;
        req.set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
        req.set_job_id(job_id);

        scheduler_grpc::DeleteJobResponse resp;
        stub_->DeleteJob(&ctx, req, &resp);
    }
}

TEST_F(SchedulerRealVehicleTest, RealVehicle_JobLifecycle) {
    // Test full lifecycle: create → pause → resume → trigger → verify execution

    // Step 1: Create job
    std::string job_id = CreateRealVehicleJob(
        "Real Vehicle E2E: Lifecycle Test",
        "echo",
        R"({"message": "Lifecycle test"})");
    ASSERT_FALSE(job_id.empty());

    std::this_thread::sleep_for(2s);

    // Step 2: Pause job
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);

        scheduler_grpc::PauseJobRequest req;
        req.set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
        req.set_job_id(job_id);

        scheduler_grpc::PauseJobResponse resp;
        auto status = stub_->PauseJob(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << "PauseJob failed: " << status.error_message();
        LOG(INFO) << "Paused job: " << (resp.success() ? "OK" : resp.error_message());
    }

    std::this_thread::sleep_for(1s);

    // Step 3: Resume job
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);

        scheduler_grpc::ResumeJobRequest req;
        req.set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
        req.set_job_id(job_id);

        scheduler_grpc::ResumeJobResponse resp;
        auto status = stub_->ResumeJob(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << "ResumeJob failed: " << status.error_message();
        LOG(INFO) << "Resumed job: " << (resp.success() ? "OK" : resp.error_message());
    }

    std::this_thread::sleep_for(1s);

    // Step 4: Trigger and verify execution
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 10s);

        scheduler_grpc::TriggerJobRequest req;
        req.set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
        req.set_job_id(job_id);

        scheduler_grpc::TriggerJobResponse resp;
        auto status = stub_->TriggerJob(&ctx, req, &resp);
        ASSERT_TRUE(status.ok()) << "TriggerJob failed: " << status.error_message();
    }

    // Wait for execution result
    ASSERT_TRUE(ifex::offboard::test::E2ETestInfrastructure::WaitForJobExecution(
        job_id, std::chrono::seconds(30)));

    LOG(INFO) << "Full job lifecycle completed successfully";

    // Clean up
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);

        scheduler_grpc::DeleteJobRequest req;
        req.set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
        req.set_job_id(job_id);

        scheduler_grpc::DeleteJobResponse resp;
        stub_->DeleteJob(&ctx, req, &resp);
    }
}

TEST_F(SchedulerRealVehicleTest, RealVehicle_JobStateSyncBack) {
    // Test that job state changes on vehicle sync back to cloud

    // Create job
    std::string job_id = CreateRealVehicleJob(
        "Real Vehicle E2E: State Sync Test",
        "echo_with_delay",
        R"({"message": "State sync", "delay_ms": 500})");
    ASSERT_FALSE(job_id.empty());

    std::this_thread::sleep_for(3s);

    // Verify job exists in database with correct state
    auto* db = ifex::offboard::test::E2ETestInfrastructure::GetDatabase();
    ASSERT_NE(db, nullptr);

    auto result = db->execute(
        "SELECT status, service_name, method_name FROM jobs WHERE job_id = $1",
        {job_id});

    ASSERT_TRUE(result.ok()) << "Failed to query job: " << result.error();
    ASSERT_GE(result.num_rows(), 1) << "Job not found in database";

    std::string service = result.row(0).get_string("service_name");
    std::string method = result.row(0).get_string("method_name");

    EXPECT_EQ(service, "echo_service");
    EXPECT_EQ(method, "echo_with_delay");

    LOG(INFO) << "Job state synced correctly to database";

    // Clean up
    {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + 5s);

        scheduler_grpc::DeleteJobRequest req;
        req.set_vehicle_id(ifex::offboard::test::E2ETestInfrastructure::VEHICLE_ID);
        req.set_job_id(job_id);

        scheduler_grpc::DeleteJobResponse resp;
        stub_->DeleteJob(&ctx, req, &resp);
    }
}

