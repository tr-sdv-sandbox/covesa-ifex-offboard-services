#pragma once

#include <atomic>
#include <memory>
#include <string>

namespace ifex {
namespace cloud {
namespace scheduler {

/// Configuration for job sync producer
struct JobSyncProducerConfig {
    std::string brokers = "localhost:9092";
    std::string topic = "ifex.c2v.scheduler";
    std::string client_id = "cloud-scheduler-producer";
};

/// Statistics for job sync producer
struct JobSyncProducerStats {
    std::atomic<uint64_t> sync_messages_sent{0};
    std::atomic<uint64_t> trigger_requests_sent{0};
    std::atomic<uint64_t> send_errors{0};
};

/// Job data for sync message
struct JobData {
    std::string job_id;
    std::string title;
    std::string service;
    std::string method;
    std::string parameters_json;
    uint64_t scheduled_time_ms = 0;
    std::string recurrence_rule;
    uint64_t end_time_ms = 0;
    bool paused = false;
    bool deleted = false;
    uint64_t cloud_seq = 0;
    uint64_t vehicle_seq = 0;
};

/**
 * Kafka producer for scheduler sync messages (pure state sync model)
 *
 * Sends C2V_SyncMessage for state replication and TriggerJobRequest for
 * immediate execution (the only imperative command).
 *
 * Produces to ifex.c2v.scheduler with vehicle_id as key.
 */
class JobSyncProducer {
public:
    explicit JobSyncProducer(JobSyncProducerConfig config);
    ~JobSyncProducer();

    /// Initialize the producer
    bool init();

    /// Send a C2V_SyncMessage with job state
    /// Used for create, update, delete, pause, resume operations
    bool send_job_sync(
        const std::string& vehicle_id,
        const JobData& job);

    /// Send a TriggerJobRequest (run job immediately)
    /// This is the only imperative command in the pure state sync model
    bool send_trigger_job(
        const std::string& vehicle_id,
        const std::string& job_id,
        const std::string& requester_id);

    /// Flush pending messages
    void flush(int timeout_ms = 5000);

    /// Get statistics
    const JobSyncProducerStats& stats() const { return stats_; }

private:
    bool send_message(const std::string& vehicle_id, const std::string& payload);

    JobSyncProducerConfig config_;
    JobSyncProducerStats stats_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
