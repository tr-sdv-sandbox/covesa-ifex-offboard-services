#pragma once

#include <functional>
#include <memory>
#include <string>

#include <librdkafka/rdkafkacpp.h>

namespace ifex::offboard {

/// Delivery report callback type
using DeliveryCallback = std::function<void(const std::string& topic,
                                            int32_t partition,
                                            int64_t offset,
                                            const std::string& error)>;

/// Configuration for Kafka producer
struct KafkaProducerConfig {
    std::string brokers = "localhost:9092";
    std::string client_id = "ifex-offboard";
    int queue_buffering_max_ms = 15;
    int message_timeout_ms = 30000;
    int max_retries = 5;
};

/// C++ wrapper for librdkafka producer
class KafkaProducer {
public:
    explicit KafkaProducer(const KafkaProducerConfig& config);
    ~KafkaProducer();

    // Non-copyable
    KafkaProducer(const KafkaProducer&) = delete;
    KafkaProducer& operator=(const KafkaProducer&) = delete;

    /// Produce a message to a topic
    /// @param topic Kafka topic name
    /// @param key Message key (used for partitioning)
    /// @param value Message value (payload)
    /// @return true if message was queued successfully
    bool produce(const std::string& topic,
                 const std::string& key,
                 const std::string& value);

    /// Produce a message with binary payload
    bool produce(const std::string& topic,
                 const std::string& key,
                 const void* value,
                 size_t value_len);

    /// Set delivery report callback
    void set_delivery_callback(DeliveryCallback callback);

    /// Poll for delivery reports (call periodically)
    void poll(int timeout_ms = 0);

    /// Flush all pending messages
    /// @param timeout_ms Maximum time to wait
    /// @return Number of messages still in queue
    int flush(int timeout_ms = 10000);

    /// Get number of messages in queue
    int queue_length() const;

private:
    class DeliveryReportCb;

    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<DeliveryReportCb> dr_cb_;
    DeliveryCallback delivery_callback_;
};

}  // namespace ifex::offboard
