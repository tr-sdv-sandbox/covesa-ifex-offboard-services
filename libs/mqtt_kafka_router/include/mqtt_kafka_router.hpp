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
    std::string topic_pattern = "v2c/#";  // Subscribe pattern for v2c

    // Topic parsing pattern: v2c/{vehicle_id}/{content_id}
    // Prefix to strip before parsing
    std::string topic_prefix = "v2c/";

    // c2v topic pattern for publishing: c2v/{vehicle_id}/{content_id}
    std::string c2v_topic_prefix = "c2v/";
};

/// Configuration for Kafka connection
struct KafkaRouterConfig {
    std::string brokers = "localhost:9092";
    std::string client_id = "mqtt-kafka-router";

    // Producer settings (for v2c)
    int queue_buffering_max_ms = 100;
    int batch_num_messages = 1000;

    // Consumer group for c2v topics
    std::string c2v_group_id = "mqtt-kafka-bridge-c2v";
};

/// Transform function signature for v2c (vehicle to cloud)
/// Returns: transformed payload bytes, or nullopt to drop the message
using TransformFn = std::function<std::optional<std::string>(
    const std::string& vehicle_id,
    uint32_t content_id,
    const std::string& payload,
    const std::optional<VehicleContext>& ctx)>;

/// Transform function signature for c2v (cloud to vehicle)
/// Input: raw Kafka message value
/// Output: vehicle_id and transformed payload, or nullopt to drop
struct C2vMessage {
    std::string vehicle_id;
    std::string payload;
};
using C2vTransformFn = std::function<std::optional<C2vMessage>(
    const std::string& kafka_key,
    const std::string& kafka_value)>;

/// Handler registration for a content_id (v2c direction)
struct ContentHandler {
    uint32_t content_id;
    std::string kafka_topic;
    TransformFn transform;
};

/// Handler registration for c2v direction
struct C2vHandler {
    std::string kafka_topic;      // Source Kafka topic
    uint32_t content_id;          // Target MQTT content_id
    C2vTransformFn transform;     // Transform function (optional)
};

/// Status callback for vehicle online/offline detection
/// Called when a vehicle publishes to v2c/{vehicle_id}/is_online
using StatusCallback = std::function<void(const std::string& vehicle_id, bool is_online)>;

/// Statistics for monitoring
struct RouterStats {
    // v2c stats (MQTT -> Kafka)
    std::atomic<uint64_t> messages_received{0};
    std::atomic<uint64_t> messages_transformed{0};
    std::atomic<uint64_t> messages_dropped{0};
    std::atomic<uint64_t> messages_produced{0};
    std::atomic<uint64_t> transform_errors{0};
    std::atomic<uint64_t> produce_errors{0};
    std::atomic<uint64_t> unknown_content_ids{0};
    std::atomic<uint64_t> unknown_vehicles{0};

    // Status (is_online) stats
    std::atomic<uint64_t> status_messages_received{0};
    std::atomic<uint64_t> status_messages_produced{0};

    // c2v stats (Kafka -> MQTT)
    std::atomic<uint64_t> c2v_messages_received{0};
    std::atomic<uint64_t> c2v_messages_transformed{0};
    std::atomic<uint64_t> c2v_messages_dropped{0};
    std::atomic<uint64_t> c2v_messages_published{0};
    std::atomic<uint64_t> c2v_transform_errors{0};
    std::atomic<uint64_t> c2v_publish_errors{0};
};

/// Bidirectional MQTT-Kafka router with content-based routing and transformation
/// - v2c: MQTT (v2c/{vehicle_id}/{content_id}) -> Kafka
/// - c2v: Kafka -> MQTT (c2v/{vehicle_id}/{content_id})
class MqttKafkaRouter {
public:
    MqttKafkaRouter(MqttRouterConfig mqtt_config, KafkaRouterConfig kafka_config);
    ~MqttKafkaRouter();

    // Non-copyable, non-movable (owns MQTT/Kafka handles)
    MqttKafkaRouter(const MqttKafkaRouter&) = delete;
    MqttKafkaRouter& operator=(const MqttKafkaRouter&) = delete;

    //------------------------------------------------------------------------
    // v2c handlers (MQTT -> Kafka)
    //------------------------------------------------------------------------

    /// Register a v2c handler for a specific content_id
    /// @param content_id The content ID to handle (from MQTT topic)
    /// @param kafka_topic Target Kafka topic for transformed messages
    /// @param transform Transform function (payload in -> payload out)
    void register_handler(uint32_t content_id, const std::string& kafka_topic,
                          TransformFn transform);

    /// Set the vehicle context store (for enrichment lookups)
    void set_context_store(std::shared_ptr<VehicleContextStore> store);

    /// Set whether to require vehicle context (drop messages for unknown vehicles)
    void set_require_context(bool require);

    /// Set the status callback for vehicle online/offline detection
    /// @param kafka_topic Kafka topic for status messages (e.g., "ifex.status")
    /// @param cb Callback invoked when status changes (for DB update)
    void set_status_handler(const std::string& kafka_topic, StatusCallback cb);

    //------------------------------------------------------------------------
    // c2v handlers (Kafka -> MQTT)
    //------------------------------------------------------------------------

    /// Register a c2v handler for a Kafka topic
    /// @param kafka_topic Source Kafka topic to consume
    /// @param content_id Target MQTT content_id for publishing
    /// @param transform Optional transform function (default: extract vehicle_id from key)
    void register_c2v_handler(const std::string& kafka_topic, uint32_t content_id,
                               C2vTransformFn transform = nullptr);

    //------------------------------------------------------------------------
    // Lifecycle
    //------------------------------------------------------------------------

    /// Start the router (blocking)
    void run();

    /// Signal the router to stop
    void stop();

    /// Get current statistics
    const RouterStats& stats() const { return stats_; }

    /// Parse MQTT topic into vehicle_id and content_id
    static std::optional<TopicInfo> parse_topic(const std::string& topic,
                                                 const std::string& prefix);

    /// Build c2v MQTT topic from vehicle_id and content_id
    std::string build_c2v_topic(const std::string& vehicle_id, uint32_t content_id) const;

private:
    void on_message(const std::string& topic, const std::string& payload);
    void process_message(const TopicInfo& info, const std::string& payload);
    void process_c2v_message(const std::string& kafka_topic, const std::string& key,
                              const std::string& value);

    MqttRouterConfig mqtt_config_;
    KafkaRouterConfig kafka_config_;

    // v2c handlers (indexed by content_id)
    std::unordered_map<uint32_t, ContentHandler> handlers_;
    std::shared_ptr<VehicleContextStore> context_store_;
    bool require_context_ = false;

    // Status handler for is_online detection
    std::string status_kafka_topic_;
    StatusCallback status_callback_;

    // c2v handlers (indexed by kafka_topic)
    std::unordered_map<std::string, C2vHandler> c2v_handlers_;

    std::atomic<bool> running_{false};
    RouterStats stats_;

    // Implementation details (pimpl)
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace ifex::offboard
