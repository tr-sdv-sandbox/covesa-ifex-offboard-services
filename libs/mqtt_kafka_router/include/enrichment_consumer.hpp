#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "vehicle_context.hpp"

namespace ifex::offboard {

/// Configuration for enrichment consumer
struct EnrichmentConsumerConfig {
    std::string brokers = "localhost:9092";
    std::string group_id = "mqtt-kafka-bridge-enrichment";
    std::string client_id = "mqtt-kafka-bridge-enrichment";
    std::string topic = "ifex.vehicle.enrichment";

    // How long to wait for initial load (blocking)
    std::chrono::milliseconds initial_load_timeout{30000};

    // Poll timeout for background updates
    int poll_timeout_ms = 1000;
};

/// Statistics for enrichment consumer
struct EnrichmentStats {
    std::atomic<uint64_t> messages_received{0};
    std::atomic<uint64_t> updates_applied{0};
    std::atomic<uint64_t> tombstones_applied{0};
    std::atomic<uint64_t> parse_errors{0};
};

/// Consumes enrichment data from Kafka and populates VehicleContextStore
/// Acts like a KTable - maintains latest value per key (vehicle_id)
class EnrichmentConsumer {
public:
    explicit EnrichmentConsumer(EnrichmentConsumerConfig config,
                                 std::shared_ptr<VehicleContextStore> store);
    ~EnrichmentConsumer();

    // Non-copyable, non-movable
    EnrichmentConsumer(const EnrichmentConsumer&) = delete;
    EnrichmentConsumer& operator=(const EnrichmentConsumer&) = delete;

    /// Perform initial load from topic beginning
    /// Blocks until topic is exhausted or timeout reached
    /// @return Number of records loaded
    size_t load_initial();

    /// Start background consumer thread for updates
    void start_background();

    /// Stop background consumer thread
    void stop();

    /// Check if initial load is complete
    bool is_loaded() const { return initial_load_complete_; }

    /// Get count of loaded contexts
    size_t context_count() const;

    /// Get statistics
    const EnrichmentStats& stats() const { return stats_; }

private:
    void process_message(const std::string& key, const std::string& value);
    void background_loop();

    EnrichmentConsumerConfig config_;
    std::shared_ptr<VehicleContextStore> store_;
    EnrichmentStats stats_;

    std::atomic<bool> initial_load_complete_{false};
    std::atomic<bool> running_{false};
    std::unique_ptr<std::thread> background_thread_;

    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ifex::offboard
