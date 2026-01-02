#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "vehicle_context.hpp"

namespace ifex::offboard {

/// Parsed MQTT topic info
struct TopicInfo {
    std::string vehicle_id;
    uint32_t content_id;
};

/// Configuration for MQTT connection
struct MqttRouterConfig {
    std::string host = "localhost";
    int port = 1883;
    std::string client_id = "mqtt-kafka-router";
    std::string username;
    std::string password;
    std::string topic_pattern = "v2c/#";  // Subscribe pattern

    // Topic parsing pattern: v2c/{vehicle_id}/{content_id}
    // Prefix to strip before parsing
    std::string topic_prefix = "v2c/";
};

/// Configuration for Kafka connection
struct KafkaRouterConfig {
    std::string brokers = "localhost:9092";
    std::string client_id = "mqtt-kafka-router";

    // Producer settings
    int queue_buffering_max_ms = 100;
    int batch_num_messages = 1000;
};

/// Transform function signature
/// Returns: transformed payload bytes, or nullopt to drop the message
using TransformFn = std::function<std::optional<std::string>(
    const std::string& vehicle_id,
    uint32_t content_id,
    const std::string& payload,
    const std::optional<VehicleContext>& ctx)>;

/// Handler registration for a content_id
struct ContentHandler {
    uint32_t content_id;
    std::string kafka_topic;
    TransformFn transform;
};

/// Statistics for monitoring
struct RouterStats {
    std::atomic<uint64_t> messages_received{0};
    std::atomic<uint64_t> messages_transformed{0};
    std::atomic<uint64_t> messages_dropped{0};
    std::atomic<uint64_t> messages_produced{0};
    std::atomic<uint64_t> transform_errors{0};
    std::atomic<uint64_t> produce_errors{0};
    std::atomic<uint64_t> unknown_content_ids{0};
    std::atomic<uint64_t> unknown_vehicles{0};
};

/// MQTT to Kafka router with content-based routing and transformation
class MqttKafkaRouter {
public:
    MqttKafkaRouter(MqttRouterConfig mqtt_config, KafkaRouterConfig kafka_config);
    ~MqttKafkaRouter();

    // Non-copyable, non-movable (owns MQTT/Kafka handles)
    MqttKafkaRouter(const MqttKafkaRouter&) = delete;
    MqttKafkaRouter& operator=(const MqttKafkaRouter&) = delete;

    /// Register a handler for a specific content_id
    /// @param content_id The content ID to handle (from MQTT topic)
    /// @param kafka_topic Target Kafka topic for transformed messages
    /// @param transform Transform function (payload in -> payload out)
    void register_handler(uint32_t content_id, const std::string& kafka_topic,
                          TransformFn transform);

    /// Set the vehicle context store (for enrichment lookups)
    void set_context_store(std::shared_ptr<VehicleContextStore> store);

    /// Set whether to require vehicle context (drop messages for unknown vehicles)
    void set_require_context(bool require);

    /// Start the router (blocking)
    void run();

    /// Signal the router to stop
    void stop();

    /// Get current statistics
    const RouterStats& stats() const { return stats_; }

    /// Parse MQTT topic into vehicle_id and content_id
    static std::optional<TopicInfo> parse_topic(const std::string& topic,
                                                 const std::string& prefix);

private:
    void on_message(const std::string& topic, const std::string& payload);
    void process_message(const TopicInfo& info, const std::string& payload);

    MqttRouterConfig mqtt_config_;
    KafkaRouterConfig kafka_config_;

    std::unordered_map<uint32_t, ContentHandler> handlers_;
    std::shared_ptr<VehicleContextStore> context_store_;
    bool require_context_ = false;

    std::atomic<bool> running_{false};
    RouterStats stats_;

    // Implementation details (pimpl)
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ifex::offboard
