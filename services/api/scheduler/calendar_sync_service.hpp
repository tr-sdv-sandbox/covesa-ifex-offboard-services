#pragma once

#include <memory>
#include <string>
#include <thread>
#include <atomic>

#include "postgres_client.hpp"
#include "kafka_consumer.hpp"
#include "job_command_producer.hpp"

namespace ifex {
namespace cloud {
namespace scheduler {

struct CalendarSyncConfig {
    std::string kafka_brokers = "localhost:9092";
    std::string status_topic = "ifex.status";
    std::string consumer_group = "scheduler-calendar-sync";
};

/**
 * CalendarSyncService - Syncs pending calendar jobs to vehicles when they come online
 *
 * Listens to ifex.status Kafka topic for vehicle online events.
 * When a vehicle comes online, queries offboard_calendar for pending jobs
 * and sends them to the vehicle via JobCommandProducer.
 */
class CalendarSyncService {
public:
    CalendarSyncService(
        const CalendarSyncConfig& config,
        std::shared_ptr<ifex::offboard::PostgresClient> db,
        std::shared_ptr<JobCommandProducer> producer);

    ~CalendarSyncService();

    // Start the background consumer thread
    bool start();

    // Stop the service
    void stop();

    // Sync pending jobs for a specific vehicle (called when vehicle comes online)
    int sync_pending_jobs(const std::string& vehicle_id);

    struct Stats {
        uint64_t vehicles_synced = 0;
        uint64_t jobs_synced = 0;
        uint64_t sync_errors = 0;
    };

    const Stats& stats() const { return stats_; }

private:
    void consumer_loop();
    void handle_status_message(const std::string& vehicle_id, const std::string& payload);

    CalendarSyncConfig config_;
    std::shared_ptr<ifex::offboard::PostgresClient> db_;
    std::shared_ptr<JobCommandProducer> producer_;

    std::unique_ptr<ifex::offboard::KafkaConsumer> consumer_;
    std::thread consumer_thread_;
    std::atomic<bool> running_{false};

    Stats stats_;
};

}  // namespace scheduler
}  // namespace cloud
}  // namespace ifex
