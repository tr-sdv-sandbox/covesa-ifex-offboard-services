/**
 * @file scheduler_command_test.cpp
 * @brief Unit tests for scheduler command serialization
 *
 * Tests that scheduler commands (create, update, delete, pause, resume, trigger)
 * serialize correctly to the scheduler-command-envelope protobuf format.
 */

#include <gtest/gtest.h>
#include <glog/logging.h>

#include "scheduler-command-envelope.pb.h"

namespace cmd = swdv::scheduler_command_envelope;

class SchedulerCommandTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize glog for tests
        static bool glog_initialized = false;
        if (!glog_initialized) {
            google::InitGoogleLogging("scheduler_command_test");
            FLAGS_logtostderr = true;
            glog_initialized = true;
        }
    }

    // Helper to serialize and deserialize a command
    cmd::scheduler_command_t RoundTrip(const cmd::scheduler_command_t& command) {
        std::string serialized = command.SerializeAsString();
        cmd::scheduler_command_t result;
        EXPECT_TRUE(result.ParseFromString(serialized));
        return result;
    }
};

// =============================================================================
// Create Job Command Tests
// =============================================================================

TEST_F(SchedulerCommandTest, CreateJobCommand) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-123");
    command.set_timestamp_ns(1234567890123456789ULL);
    command.set_requester_id("cloud-scheduler");
    command.set_type(cmd::COMMAND_CREATE_JOB);

    auto* job = command.mutable_create_job();
    job->set_job_id("job-456");
    job->set_title("Daily HVAC Check");
    job->set_service("climate_service");
    job->set_method("get_status");
    job->set_parameters_json(R"({"zone": "cabin"})");
    job->set_scheduled_time("2026-01-05T08:00:00Z");
    job->set_recurrence_rule("0 8 * * *");  // Daily at 8am
    job->set_end_time("2026-12-31T23:59:59Z");

    // Round-trip serialization
    auto result = RoundTrip(command);

    EXPECT_EQ(result.command_id(), "cmd-123");
    EXPECT_EQ(result.timestamp_ns(), 1234567890123456789ULL);
    EXPECT_EQ(result.requester_id(), "cloud-scheduler");
    EXPECT_EQ(result.type(), cmd::COMMAND_CREATE_JOB);

    ASSERT_TRUE(result.has_create_job());
    const auto& create = result.create_job();
    EXPECT_EQ(create.job_id(), "job-456");
    EXPECT_EQ(create.title(), "Daily HVAC Check");
    EXPECT_EQ(create.service(), "climate_service");
    EXPECT_EQ(create.method(), "get_status");
    EXPECT_EQ(create.parameters_json(), R"({"zone": "cabin"})");
    EXPECT_EQ(create.scheduled_time(), "2026-01-05T08:00:00Z");
    EXPECT_EQ(create.recurrence_rule(), "0 8 * * *");
    EXPECT_EQ(create.end_time(), "2026-12-31T23:59:59Z");
}

TEST_F(SchedulerCommandTest, CreateJobWithMinimalFields) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-min");
    command.set_type(cmd::COMMAND_CREATE_JOB);

    auto* job = command.mutable_create_job();
    job->set_job_id("job-min");
    job->set_title("Simple Job");
    job->set_service("echo_service");
    job->set_method("echo");
    job->set_scheduled_time("2026-01-05T09:00:00Z");
    // No recurrence_rule, end_time, parameters_json

    auto result = RoundTrip(command);

    EXPECT_EQ(result.type(), cmd::COMMAND_CREATE_JOB);
    const auto& create = result.create_job();
    EXPECT_EQ(create.job_id(), "job-min");
    EXPECT_EQ(create.recurrence_rule(), "");  // Empty
    EXPECT_EQ(create.end_time(), "");
    EXPECT_EQ(create.parameters_json(), "");
}

// =============================================================================
// Update Job Command Tests
// =============================================================================

TEST_F(SchedulerCommandTest, UpdateJobCommand) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-update");
    command.set_type(cmd::COMMAND_UPDATE_JOB);

    auto* update = command.mutable_update_job();
    update->set_job_id("job-456");
    update->set_title("Updated HVAC Check");
    update->set_scheduled_time("2026-01-06T09:00:00Z");
    update->set_recurrence_rule("0 9 * * MON-FRI");  // Weekdays only
    update->set_parameters_json(R"({"zone": "all"})");

    auto result = RoundTrip(command);

    EXPECT_EQ(result.type(), cmd::COMMAND_UPDATE_JOB);
    ASSERT_TRUE(result.has_update_job());
    const auto& upd = result.update_job();
    EXPECT_EQ(upd.job_id(), "job-456");
    EXPECT_EQ(upd.title(), "Updated HVAC Check");
    EXPECT_EQ(upd.scheduled_time(), "2026-01-06T09:00:00Z");
    EXPECT_EQ(upd.recurrence_rule(), "0 9 * * MON-FRI");
    EXPECT_EQ(upd.parameters_json(), R"({"zone": "all"})");
}

// =============================================================================
// Delete Job Command Tests
// =============================================================================

TEST_F(SchedulerCommandTest, DeleteJobCommand) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-delete");
    command.set_type(cmd::COMMAND_DELETE_JOB);
    command.set_delete_job_id("job-to-delete");

    auto result = RoundTrip(command);

    EXPECT_EQ(result.type(), cmd::COMMAND_DELETE_JOB);
    EXPECT_EQ(result.delete_job_id(), "job-to-delete");
}

// =============================================================================
// Pause Job Command Tests
// =============================================================================

TEST_F(SchedulerCommandTest, PauseJobCommand) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-pause");
    command.set_timestamp_ns(9999999999ULL);
    command.set_requester_id("fleet-manager");
    command.set_type(cmd::COMMAND_PAUSE_JOB);
    command.set_pause_job_id("job-to-pause");

    auto result = RoundTrip(command);

    EXPECT_EQ(result.command_id(), "cmd-pause");
    EXPECT_EQ(result.timestamp_ns(), 9999999999ULL);
    EXPECT_EQ(result.requester_id(), "fleet-manager");
    EXPECT_EQ(result.type(), cmd::COMMAND_PAUSE_JOB);
    EXPECT_EQ(result.pause_job_id(), "job-to-pause");
}

TEST_F(SchedulerCommandTest, PauseJobCommandType) {
    // Verify the command type enum value
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_PAUSE_JOB), 4);
}

// =============================================================================
// Resume Job Command Tests
// =============================================================================

TEST_F(SchedulerCommandTest, ResumeJobCommand) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-resume");
    command.set_timestamp_ns(8888888888ULL);
    command.set_requester_id("fleet-manager");
    command.set_type(cmd::COMMAND_RESUME_JOB);
    command.set_resume_job_id("job-to-resume");

    auto result = RoundTrip(command);

    EXPECT_EQ(result.command_id(), "cmd-resume");
    EXPECT_EQ(result.timestamp_ns(), 8888888888ULL);
    EXPECT_EQ(result.requester_id(), "fleet-manager");
    EXPECT_EQ(result.type(), cmd::COMMAND_RESUME_JOB);
    EXPECT_EQ(result.resume_job_id(), "job-to-resume");
}

TEST_F(SchedulerCommandTest, ResumeJobCommandType) {
    // Verify the command type enum value
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_RESUME_JOB), 5);
}

// =============================================================================
// Trigger Job Command Tests
// =============================================================================

TEST_F(SchedulerCommandTest, TriggerJobCommand) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-trigger");
    command.set_timestamp_ns(7777777777ULL);
    command.set_requester_id("emergency-override");
    command.set_type(cmd::COMMAND_TRIGGER_JOB);
    command.set_trigger_job_id("job-to-trigger-now");

    auto result = RoundTrip(command);

    EXPECT_EQ(result.command_id(), "cmd-trigger");
    EXPECT_EQ(result.timestamp_ns(), 7777777777ULL);
    EXPECT_EQ(result.requester_id(), "emergency-override");
    EXPECT_EQ(result.type(), cmd::COMMAND_TRIGGER_JOB);
    EXPECT_EQ(result.trigger_job_id(), "job-to-trigger-now");
}

TEST_F(SchedulerCommandTest, TriggerJobCommandType) {
    // Verify the command type enum value
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_TRIGGER_JOB), 6);
}

// =============================================================================
// Command Type Enum Tests
// =============================================================================

TEST_F(SchedulerCommandTest, CommandTypeEnumValues) {
    // Verify all command type values match expected
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_UNKNOWN), 0);
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_CREATE_JOB), 1);
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_UPDATE_JOB), 2);
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_DELETE_JOB), 3);
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_PAUSE_JOB), 4);
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_RESUME_JOB), 5);
    EXPECT_EQ(static_cast<int>(cmd::COMMAND_TRIGGER_JOB), 6);
}

// =============================================================================
// Command Acknowledgment Tests
// =============================================================================

TEST_F(SchedulerCommandTest, CommandAckSuccess) {
    cmd::scheduler_command_ack_t ack;
    ack.set_command_id("cmd-123");
    ack.set_success(true);
    ack.set_job_id("job-456");
    ack.set_timestamp_ns(1234567890ULL);

    std::string serialized = ack.SerializeAsString();
    cmd::scheduler_command_ack_t result;
    ASSERT_TRUE(result.ParseFromString(serialized));

    EXPECT_EQ(result.command_id(), "cmd-123");
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.job_id(), "job-456");
    EXPECT_EQ(result.timestamp_ns(), 1234567890ULL);
    EXPECT_TRUE(result.error_message().empty());
}

TEST_F(SchedulerCommandTest, CommandAckFailure) {
    cmd::scheduler_command_ack_t ack;
    ack.set_command_id("cmd-789");
    ack.set_success(false);
    ack.set_error_message("Job not found");

    std::string serialized = ack.SerializeAsString();
    cmd::scheduler_command_ack_t result;
    ASSERT_TRUE(result.ParseFromString(serialized));

    EXPECT_EQ(result.command_id(), "cmd-789");
    EXPECT_FALSE(result.success());
    EXPECT_EQ(result.error_message(), "Job not found");
}

// =============================================================================
// Serialization Size Tests
// =============================================================================

TEST_F(SchedulerCommandTest, PauseCommandIsCompact) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-pause-001");
    command.set_type(cmd::COMMAND_PAUSE_JOB);
    command.set_pause_job_id("job-001");

    std::string serialized = command.SerializeAsString();

    // Pause command should be very compact (< 50 bytes)
    EXPECT_LT(serialized.size(), 50) << "Pause command should be compact";
    LOG(INFO) << "Pause command size: " << serialized.size() << " bytes";
}

TEST_F(SchedulerCommandTest, ResumeCommandIsCompact) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-resume-001");
    command.set_type(cmd::COMMAND_RESUME_JOB);
    command.set_resume_job_id("job-001");

    std::string serialized = command.SerializeAsString();

    // Resume command should be very compact (< 50 bytes)
    EXPECT_LT(serialized.size(), 50) << "Resume command should be compact";
    LOG(INFO) << "Resume command size: " << serialized.size() << " bytes";
}

TEST_F(SchedulerCommandTest, TriggerCommandIsCompact) {
    cmd::scheduler_command_t command;
    command.set_command_id("cmd-trigger-001");
    command.set_type(cmd::COMMAND_TRIGGER_JOB);
    command.set_trigger_job_id("job-001");

    std::string serialized = command.SerializeAsString();

    // Trigger command should be very compact (< 50 bytes)
    EXPECT_LT(serialized.size(), 50) << "Trigger command should be compact";
    LOG(INFO) << "Trigger command size: " << serialized.size() << " bytes";
}

// =============================================================================
// Workflow Tests
// =============================================================================

TEST_F(SchedulerCommandTest, PauseResumeWorkflow) {
    // Simulate a pause/resume workflow
    std::string job_id = "workflow-job";
    std::vector<cmd::scheduler_command_t> workflow;

    // 1. Pause the job
    {
        cmd::scheduler_command_t cmd;
        cmd.set_command_id("workflow-cmd-1");
        cmd.set_type(cmd::COMMAND_PAUSE_JOB);
        cmd.set_pause_job_id(job_id);
        workflow.push_back(cmd);
    }

    // 2. Resume the job
    {
        cmd::scheduler_command_t cmd;
        cmd.set_command_id("workflow-cmd-2");
        cmd.set_type(cmd::COMMAND_RESUME_JOB);
        cmd.set_resume_job_id(job_id);
        workflow.push_back(cmd);
    }

    // 3. Trigger immediate execution
    {
        cmd::scheduler_command_t cmd;
        cmd.set_command_id("workflow-cmd-3");
        cmd.set_type(cmd::COMMAND_TRIGGER_JOB);
        cmd.set_trigger_job_id(job_id);
        workflow.push_back(cmd);
    }

    // Verify workflow can be serialized and deserialized
    for (size_t i = 0; i < workflow.size(); i++) {
        auto result = RoundTrip(workflow[i]);
        EXPECT_EQ(result.command_id(), workflow[i].command_id())
            << "Workflow step " << i << " failed round-trip";
    }

    EXPECT_EQ(workflow[0].type(), cmd::COMMAND_PAUSE_JOB);
    EXPECT_EQ(workflow[1].type(), cmd::COMMAND_RESUME_JOB);
    EXPECT_EQ(workflow[2].type(), cmd::COMMAND_TRIGGER_JOB);
}
