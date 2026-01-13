#pragma once

/// @file kafka_mqtt_transport.hpp
/// @brief Kafka+MQTT implementation of OffboardTransport.
///
/// This implementation:
/// - Consumes v2c messages from Kafka (ingested by mqtt_kafka_bridge)
/// - Produces c2v messages to Kafka (mqtt_kafka_bridge forwards to MQTT)
/// - Tracks vehicle online/offline status from Kafka status topic

#include "offboard_transport.hpp"
#include "kafka_producer.hpp"
#include "kafka_consumer.hpp"

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <thread>

namespace ifex::offboard {

/// Kafka+MQTT implementation of OffboardTransport
class KafkaMqttTransport : public OffboardTransport {
public:
    explicit KafkaMqttTransport(const OffboardTransportConfig& config);
    ~KafkaMqttTransport() override;

    // Non-copyable
    KafkaMqttTransport(const KafkaMqttTransport&) = delete;
    KafkaMqttTransport& operator=(const KafkaMqttTransport&) = delete;

    // =========================================================================
    // Lifecycle
    // =========================================================================

    bool Start() override;
    void Stop() override;
    bool IsRunning() const override { return running_; }

    // =========================================================================
    // Methods
    // =========================================================================

    SendResponse SendToVehicle(const SendRequest& request) override;
    VehicleStatus GetVehicleStatus(const std::string& vehicle_id) const override;
    int64_t GetVehicleLastSeen(const std::string& vehicle_id) const override;
    ChannelInfo GetChannelInfo() const override;
    QueueStatusInfo GetQueueStatus(const std::string& vehicle_id) const override;
    TransportStats GetStats() const override;
    bool IsHealthy() const override;

    // =========================================================================
    // Callbacks
    // =========================================================================

    void SetOnVehicleMessage(VehicleMessageCallback callback) override;
    void SetOnAck(AckCallback callback) override;
    void SetOnVehicleStatus(VehicleStatusCallback callback) override;
    void SetOnQueueStatusChanged(QueueStatusCallback callback) override;

private:
    // =========================================================================
    // Internal Types
    // =========================================================================

    struct VehicleState {
        VehicleStatus status = VehicleStatus::UNKNOWN;
        int64_t last_seen_ms = 0;
        uint64_t next_sequence = 1;      // Next outbound sequence number
        uint32_t pending_messages = 0;   // Messages awaiting ack
    };

    // =========================================================================
    // Internal Methods
    // =========================================================================

    /// Consumer thread: reads from Kafka v2c topic
    void ConsumerThread();

    /// Status consumer thread: reads from Kafka status topic
    void StatusConsumerThread();

    /// Handle incoming v2c message from Kafka
    void HandleV2CMessage(const KafkaMessage& kafka_msg);

    /// Handle vehicle status update from Kafka
    void HandleStatusMessage(const KafkaMessage& kafka_msg);

    /// Get or create vehicle state
    VehicleState& GetOrCreateVehicleState(const std::string& vehicle_id);

    /// Check if vehicle belongs to this partition
    bool IsVehicleInPartition(const std::string& vehicle_id) const;

    /// Compute partition for a vehicle ID
    uint32_t ComputePartition(const std::string& vehicle_id) const;

    /// Get current time in milliseconds
    static int64_t CurrentTimeMs();

    // =========================================================================
    // Configuration
    // =========================================================================

    OffboardTransportConfig config_;

    // Kafka topic names
    std::string v2c_topic_;      // e.g., "ifex.scheduler.202" for content_id=202
    std::string c2v_topic_;      // e.g., "ifex.c2v.202"
    std::string status_topic_;   // e.g., "ifex.status"

    // =========================================================================
    // Kafka Clients
    // =========================================================================

    std::unique_ptr<KafkaProducer> producer_;
    std::unique_ptr<KafkaConsumer> v2c_consumer_;
    std::unique_ptr<KafkaConsumer> status_consumer_;

    // =========================================================================
    // Threads
    // =========================================================================

    std::thread consumer_thread_;
    std::thread status_thread_;
    std::atomic<bool> running_{false};

    // =========================================================================
    // State
    // =========================================================================

    mutable std::mutex state_mutex_;
    std::map<std::string, VehicleState> vehicle_states_;

    // Statistics
    mutable std::mutex stats_mutex_;
    TransportStats stats_;

    // =========================================================================
    // Callbacks
    // =========================================================================

    mutable std::mutex callback_mutex_;
    VehicleMessageCallback on_message_cb_;
    AckCallback on_ack_cb_;
    VehicleStatusCallback on_status_cb_;
    QueueStatusCallback on_queue_cb_;
};

}  // namespace ifex::offboard
