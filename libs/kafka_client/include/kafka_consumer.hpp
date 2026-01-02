#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <librdkafka/rdkafkacpp.h>

namespace ifex::offboard {

/// Message received from Kafka
struct KafkaMessage {
    std::string topic;
    int32_t partition;
    int64_t offset;
    std::string key;
    std::string value;
    int64_t timestamp;
};

/// Message handler callback
using MessageCallback = std::function<void(const KafkaMessage& message)>;

/// Configuration for Kafka consumer
struct KafkaConsumerConfig {
    std::string brokers = "localhost:9092";
    std::string group_id = "ifex-offboard";
    std::string client_id = "ifex-offboard-consumer";
    std::string auto_offset_reset = "earliest";
    bool enable_auto_commit = false;
    int auto_commit_interval_ms = 5000;
    int session_timeout_ms = 30000;
};

/// C++ wrapper for librdkafka consumer
class KafkaConsumer {
public:
    explicit KafkaConsumer(const KafkaConsumerConfig& config);
    ~KafkaConsumer();

    // Non-copyable
    KafkaConsumer(const KafkaConsumer&) = delete;
    KafkaConsumer& operator=(const KafkaConsumer&) = delete;

    /// Subscribe to topics
    bool subscribe(const std::vector<std::string>& topics);

    /// Unsubscribe from all topics
    void unsubscribe();

    /// Poll for a message
    /// @param timeout_ms Maximum time to wait
    /// @param message Output message (only valid if true returned)
    /// @return true if a message was received
    bool poll(int timeout_ms, KafkaMessage& message);

    /// Consume messages in a loop
    /// @param callback Called for each message
    /// @param timeout_ms Poll timeout
    void consume_loop(MessageCallback callback, int timeout_ms = 1000);

    /// Stop consume loop
    void stop();

    /// Commit current offsets synchronously
    bool commit();

    /// Commit specific message offset
    bool commit(const KafkaMessage& message);

    /// Get current subscribed topics
    std::vector<std::string> get_subscription() const;

private:
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    bool running_ = false;
};

}  // namespace ifex::offboard
