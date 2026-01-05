#pragma once

#include <atomic>
#include <memory>
#include <string>

namespace ifex {
namespace cloud {
namespace scheduler {

/// Job command types
enum class JobCommandType {
    CREATE = 0,
    UPDATE = 1,
    DELETE = 2,
    PAUSE = 3,
    RESUME = 4,
    TRIGGER = 5
};

/// Configuration for job command producer
struct JobCommandProducerConfig {
    std::string brokers = "localhost:9092";
    std::string topic = "ifex.c2v.scheduler";
    std::string client_id = "cloud-scheduler-producer";
};

/// Statistics for job command producer
struct JobCommandProducerStats {
    std::atomic<uint64_t> commands_sent{0};
    std::atomic<uint64_t> send_errors{0};
};

/**
 * Kafka producer for scheduler commands
 *
 * Produces to ifex.c2v.scheduler with vehicle_id as key
 */
class JobCommandProducer {
public:
    explicit JobCommandProducer(JobCommandProducerConfig config);
    ~JobCommandProducer();

    /// Initialize the producer
    bool init();

    /// Send a create job command
    bool send_create_job(
        const std::string& vehicle_id,
        const std::string& command_id,
        const std::string& job_id,
        const std::string& title,
        const std::string& service_name,
        const std::string& method_name,
        const std::string& parameters_json,
        const std::string& scheduled_time,
        const std::string& recurrence_rule,
        const std::string& end_time,
        const std::string& requester_id);

    /// Send an update job command
    bool send_update_job(
        const std::string& vehicle_id,
        const std::string& command_id,
        const std::string& job_id,
        const std::string& title,
        const std::string& scheduled_time,
        const std::string& recurrence_rule,
        const std::string& parameters_json,
        const std::string& end_time,
        const std::string& requester_id);

    /// Send a delete job command
    bool send_delete_job(
        const std::string& vehicle_id,
        const std::string& command_id,
        const std::string& job_id,
        const std::string& requester_id);

    /// Send a pause job command
    bool send_pause_job(
        const std::string& vehicle_id,
        const std::string& command_id,
        const std::string& job_id,
        const std::string& requester_id);

    /// Send a resume job command
    bool send_resume_job(
        const std::string& vehicle_id,
        const std::string& command_id,
        const std::string& job_id,
        const std::string& requester_id);

    /// Send a trigger job command (run immediately)
    bool send_trigger_job(
        const std::string& vehicle_id,
        const std::string& command_id,
        const std::string& job_id,
        const std::string& requester_id);

    /// Flush pending messages
    void flush(int timeout_ms = 5000);

    /// Get statistics
    const JobCommandProducerStats& stats() const { return stats_; }

private:
    bool send_command(const std::string& vehicle_id, const std::string& payload);

    JobCommandProducerConfig config_;
    JobCommandProducerStats stats_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
