#pragma once

/// @file offboard_transport.hpp
/// @brief Abstract interface for cloud-side vehicle transport.
///
/// This interface matches the IFEX cloud_backend_transport_service specification.
/// Implementations can use different backends (Kafka+MQTT, simple MQTT, mock).

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace ifex::offboard {

// =============================================================================
// Enumerations (from IFEX spec)
// =============================================================================

/// Message persistence level (all levels preserve ordering)
enum class Persistence : uint8_t {
    BEST_EFFORT = 0,  ///< Queued for ordering, but pruned if stale. No retry.
    VOLATILE = 1,     ///< Retry until delivered. Kept in memory, lost on shutdown.
    DURABLE = 2       ///< Retry until delivered. Persisted on graceful shutdown.
};

/// Result of send operation
enum class PublishStatus : uint8_t {
    OK = 0,              ///< Message accepted into queue
    QUEUE_FULL = 1,      ///< Queue at capacity, message rejected
    MESSAGE_TOO_LONG = 2,///< Payload exceeds maximum size
    INVALID_REQUEST = 3, ///< Invalid vehicle_id or parameters
    VEHICLE_UNKNOWN = 4, ///< Vehicle has never been seen
    WRONG_PARTITION = 5  ///< Vehicle belongs to a different partition
};

/// Queue fill level for adaptive throttling
enum class QueueLevel : uint8_t {
    EMPTY = 0,
    LOW = 1,
    NORMAL = 2,
    HIGH = 3,     ///< Consider throttling
    CRITICAL = 4, ///< Throttle non-essential
    FULL = 5
};

/// Vehicle connection status
enum class VehicleStatus : uint8_t {
    UNKNOWN = 0, ///< Vehicle has never been seen
    ONLINE = 1,  ///< Vehicle is connected
    OFFLINE = 2  ///< Vehicle disconnected (LWT received or timeout)
};

// =============================================================================
// Data Structures (from IFEX spec)
// =============================================================================

/// Optional transport-provided metadata
struct TransportMetadata {
    std::string originator;       ///< Source identifier (e.g., backend_transport)
    uint64_t message_id = 0;      ///< Transport-layer message ID
    int64_t received_at_ms = 0;   ///< Timestamp when message was received
    std::string source_topic;     ///< Original MQTT/Kafka topic (for debugging)
};

/// Request to send message to a vehicle
struct SendRequest {
    std::string vehicle_id;        ///< Target vehicle identifier
    std::vector<uint8_t> payload;  ///< Binary payload (opaque - caller handles encoding)
    Persistence persistence = Persistence::BEST_EFFORT;
};

/// Result of send operation (returned immediately, non-blocking)
struct SendResponse {
    uint64_t sequence = 0;         ///< Per-vehicle sequence number
    PublishStatus status = PublishStatus::OK;
    QueueLevel queue_level = QueueLevel::NORMAL;
};

/// Message received from a vehicle
struct VehicleMessage {
    std::string vehicle_id;        ///< Source vehicle identifier
    std::vector<uint8_t> payload;  ///< Binary payload
    uint64_t sequence = 0;         ///< Vehicle's outbound sequence number
    int64_t timestamp_ms = 0;      ///< Message timestamp (ms since epoch)
    std::optional<TransportMetadata> metadata;
};

/// Delivery confirmation for a message sent to vehicle
struct DeliveryAck {
    std::string vehicle_id;        ///< Vehicle that received the message
    uint64_t sequence = 0;         ///< Sequence number that was delivered
};

/// Vehicle online/offline status change
struct VehicleStatusEvent {
    std::string vehicle_id;
    VehicleStatus status = VehicleStatus::UNKNOWN;
    int64_t timestamp_ms = 0;
    int64_t last_seen_ms = 0;      ///< Last message timestamp from this vehicle
};

/// Per-vehicle outbound queue status for adaptive throttling
struct QueueStatusInfo {
    std::string vehicle_id;        ///< Vehicle this queue is for
    QueueLevel level = QueueLevel::NORMAL;
    uint32_t queue_size = 0;
    uint32_t queue_capacity = 0;
};

/// Channel binding info for this instance
struct ChannelInfo {
    uint32_t content_id = 0;       ///< Content type this channel handles
    uint32_t partition_id = 0;     ///< Partition this instance is bound to
    uint32_t total_partitions = 1; ///< Total number of partitions
};

/// Transport statistics (for this partition)
struct TransportStats {
    uint64_t messages_sent = 0;
    uint64_t messages_failed = 0;
    uint64_t bytes_sent = 0;
    uint64_t messages_received = 0;
    uint64_t bytes_received = 0;
    uint32_t vehicles_online = 0;  ///< Number of currently online vehicles
    uint32_t vehicles_total = 0;   ///< Total known vehicles in this partition
};

// =============================================================================
// Callback Types
// =============================================================================

/// Callback for message received from a vehicle
using VehicleMessageCallback = std::function<void(const VehicleMessage&)>;

/// Callback for delivery acknowledgment
using AckCallback = std::function<void(const DeliveryAck&)>;

/// Callback for vehicle status change
using VehicleStatusCallback = std::function<void(const VehicleStatusEvent&)>;

/// Callback for queue status change
using QueueStatusCallback = std::function<void(const QueueStatusInfo&)>;

// =============================================================================
// Configuration
// =============================================================================

/// Configuration for OffboardTransport
struct OffboardTransportConfig {
    uint32_t content_id = 0;           ///< Content ID this transport handles
    uint32_t partition_id = 0;         ///< Partition assignment (0 for single-partition)
    uint32_t total_partitions = 1;     ///< Total partitions (1 for non-partitioned)

    // Kafka settings
    std::string kafka_broker = "localhost:9092";
    std::string kafka_group_id = "offboard-transport";

    // Kafka topic names (must match mqtt_kafka_bridge configuration)
    // v2c: Topic that mqtt_kafka_bridge produces to (we consume)
    // c2v: Topic that mqtt_kafka_bridge consumes from (we produce)
    // If empty, defaults to content_id-based naming: ifex.v2c.{content_id}, ifex.c2v.{content_id}
    std::string kafka_topic_v2c;       ///< e.g., "ifex.scheduler.202"
    std::string kafka_topic_c2v;       ///< e.g., "ifex.c2v.scheduler"
    std::string kafka_topic_status = "ifex.status";  ///< Vehicle status topic

    // MQTT settings (for c2v via mqtt_kafka_bridge → MQTT)
    std::string mqtt_host = "localhost";
    uint16_t mqtt_port = 1883;

    // Queue settings
    uint32_t max_queue_size = 10000;
    uint32_t max_payload_size = 1024 * 1024;  // 1MB
};

// =============================================================================
// Abstract Interface
// =============================================================================

/// Abstract interface for cloud-side vehicle transport.
///
/// Matches the IFEX cloud_backend_transport_service API.
/// Each instance is bound to ONE content_id and ONE partition.
class OffboardTransport {
public:
    virtual ~OffboardTransport() = default;

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /// Start the transport (connect to Kafka/MQTT, start processing)
    /// @return true if started successfully
    virtual bool Start() = 0;

    /// Stop the transport gracefully
    virtual void Stop() = 0;

    /// Check if transport is running
    virtual bool IsRunning() const = 0;

    // =========================================================================
    // Methods (from IFEX spec)
    // =========================================================================

    /// Queue message for delivery to a specific vehicle. Non-blocking.
    /// Vehicle must belong to this partition (returns WRONG_PARTITION otherwise).
    /// Messages are queued if vehicle is offline (per persistence level).
    /// Use OnAck callback to track delivery confirmation.
    virtual SendResponse SendToVehicle(const SendRequest& request) = 0;

    /// Query current status of a vehicle
    virtual VehicleStatus GetVehicleStatus(const std::string& vehicle_id) const = 0;

    /// Get last seen timestamp for a vehicle (ms since epoch, 0 if never seen)
    virtual int64_t GetVehicleLastSeen(const std::string& vehicle_id) const = 0;

    /// Get channel binding info for this instance
    virtual ChannelInfo GetChannelInfo() const = 0;

    /// Get outbound queue status for a specific vehicle
    virtual QueueStatusInfo GetQueueStatus(const std::string& vehicle_id) const = 0;

    /// Get transport statistics
    virtual TransportStats GetStats() const = 0;

    /// Check if transport is healthy (connected to Kafka/MQTT)
    virtual bool IsHealthy() const = 0;

    // =========================================================================
    // Event Callbacks (from IFEX spec)
    // =========================================================================

    /// Set callback for messages received from vehicles.
    /// Only delivers messages for vehicles assigned to this partition.
    virtual void SetOnVehicleMessage(VehicleMessageCallback callback) = 0;

    /// Set callback for delivery confirmations.
    /// Only successful deliveries generate acks.
    virtual void SetOnAck(AckCallback callback) = 0;

    /// Set callback for vehicle status changes.
    /// Triggered by MQTT LWT or heartbeat timeout.
    virtual void SetOnVehicleStatus(VehicleStatusCallback callback) = 0;

    /// Set callback for queue status changes.
    /// Subscribe for adaptive throttling feedback.
    virtual void SetOnQueueStatusChanged(QueueStatusCallback callback) = 0;
};

/// Factory function to create transport implementation
/// @param config Transport configuration
/// @return Unique pointer to transport implementation
std::unique_ptr<OffboardTransport> CreateKafkaMqttTransport(
    const OffboardTransportConfig& config);

}  // namespace ifex::offboard
