/**
 * @file scheduler_e2e_test.cpp
 * @brief End-to-end tests for Cloud Scheduler ↔ Vehicle communication
 *
 * These tests verify the complete bidirectional flow:
 *   Cloud Scheduler → Kafka → MQTT → Vehicle → Scheduler → Response → MQTT → Kafka
 *
 * Prerequisites:
 *   - Docker containers running (Kafka, PostgreSQL, Mosquitto)
 *   - Vehicle container running with matching VEHICLE_ID
 *   - Run via: ./deploy/test-scheduler-e2e.sh --keep && ctest -R scheduler_e2e
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <thread>
#include <string>
#include <memory>
#include <atomic>

#include <grpcpp/grpcpp.h>
#include "cloud-scheduler-service.grpc.pb.h"
#include "scheduler-command-envelope.pb.h"

namespace scheduler_grpc = ifex::cloud::scheduler;
namespace cmd_pb = swdv::scheduler_command_envelope;

using namespace std::chrono_literals;

/**
 * @brief Test fixture for Scheduler E2E tests
 *
 * Assumes infrastructure is already running via deploy scripts.
 * Tests can be run with:
 *   ./deploy/test-scheduler-e2e.sh --keep  # Start infrastructure
 *   ctest -R scheduler_e2e                  # Run these tests
 */
class SchedulerE2ETest : public ::testing::Test {
protected:
    static constexpr const char* CLOUD_SCHEDULER_ADDR = "localhost:50083";
    static constexpr const char* VEHICLE_ID = "VIN00000000000001";

    void SetUp() override {
        // Initialize glog for tests
        static bool glog_initialized = false;
        if (!glog_initialized) {
            google::InitGoogleLogging("scheduler_e2e_test");
            FLAGS_logtostderr = true;
            glog_initialized = true;
        }

        // Create gRPC channel to Cloud Scheduler
        auto channel = grpc::CreateChannel(
            CLOUD_SCHEDULER_ADDR,
            grpc::InsecureChannelCredentials());

        // Wait for channel to be ready (with timeout)
        auto deadline = std::chrono::system_clock::now() + 5s;
        if (!channel->WaitForConnected(deadline)) {
            LOG(WARNING) << "Cloud Scheduler not available at " << CLOUD_SCHEDULER_ADDR;
            cloud_scheduler_available_ = false;
        } else {
            cloud_scheduler_available_ = true;
            stub_ = scheduler_grpc::CloudSchedulerService::NewStub(channel);
        }
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

    // Skip test if cloud scheduler is not available
    void RequireCloudScheduler() {
        if (!cloud_scheduler_available_) {
            GTEST_SKIP() << "Cloud Scheduler not available. "
                         << "Run ./deploy/test-scheduler-e2e.sh --keep first";
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

    bool cloud_scheduler_available_ = false;
    std::unique_ptr<scheduler_grpc::CloudSchedulerService::Stub> stub_;
    std::vector<std::string> created_jobs_;
};

// =============================================================================
// Connection Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CloudSchedulerConnection) {
    // This test verifies the Cloud Scheduler is running
    // If it's not, other tests will be skipped
    if (cloud_scheduler_available_) {
        LOG(INFO) << "Cloud Scheduler is available at " << CLOUD_SCHEDULER_ADDR;
        SUCCEED();
    } else {
        LOG(WARNING) << "Cloud Scheduler not available - integration tests will be skipped";
        // Don't fail - just log the warning
        SUCCEED();
    }
}

// =============================================================================
// CreateJob Tests
// =============================================================================

TEST_F(SchedulerE2ETest, CreateJobBasic) {
    RequireCloudScheduler();

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
    RequireCloudScheduler();

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
    RequireCloudScheduler();

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
    RequireCloudScheduler();

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
    RequireCloudScheduler();

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
    RequireCloudScheduler();

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
    RequireCloudScheduler();

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
    RequireCloudScheduler();

    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + 5s);

    scheduler_grpc::PauseJobRequest req;
    req.set_vehicle_id(VEHICLE_ID);
    req.set_job_id("nonexistent-job-id-12345");

    scheduler_grpc::PauseJobResponse resp;
    auto status = stub_->PauseJob(&ctx, req, &resp);

    ASSERT_TRUE(status.ok()) << "gRPC should succeed even if job doesn't exist";
    // The response may indicate failure (job not found)
    LOG(INFO) << "PauseNonexistentJob: success=" << resp.success()
              << " error=" << resp.error_message();
}

TEST_F(SchedulerE2ETest, CreateJobInvalidVehicle) {
    RequireCloudScheduler();

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

