#include "kafka_mqtt_transport.hpp"

#include <glog/logging.h>

#include <chrono>
#include <functional>

namespace ifex::offboard {

// =============================================================================
// Constructor / Destructor
// =============================================================================

KafkaMqttTransport::KafkaMqttTransport(const OffboardTransportConfig& config)
    : config_(config) {
    // Use configured topic names or default to content_id-based naming
    if (config_.kafka_topic_v2c.empty()) {
        v2c_topic_ = "ifex.v2c." + std::to_string(config_.content_id);
    } else {
        v2c_topic_ = config_.kafka_topic_v2c;
    }

    if (config_.kafka_topic_c2v.empty()) {
        c2v_topic_ = "ifex.c2v." + std::to_string(config_.content_id);
    } else {
        c2v_topic_ = config_.kafka_topic_c2v;
    }

    status_topic_ = config_.kafka_topic_status;

    LOG(INFO) << "KafkaMqttTransport created for content_id=" << config_.content_id
              << " partition=" << config_.partition_id << "/" << config_.total_partitions;
    LOG(INFO) << "  v2c topic: " << v2c_topic_;
    LOG(INFO) << "  c2v topic: " << c2v_topic_;
    LOG(INFO) << "  status topic: " << status_topic_;
}

KafkaMqttTransport::~KafkaMqttTransport() {
    Stop();
}

// =============================================================================
// Lifecycle
// =============================================================================

bool KafkaMqttTransport::Start() {
    if (running_) {
        LOG(WARNING) << "KafkaMqttTransport already running";
        return true;
    }

    LOG(INFO) << "Starting KafkaMqttTransport...";

    try {
        // Create Kafka producer for c2v messages
        KafkaProducerConfig producer_config;
        producer_config.brokers = config_.kafka_broker;
        producer_config.client_id = "offboard-transport-" + std::to_string(config_.content_id);
        producer_ = std::make_unique<KafkaProducer>(producer_config);

        // Set delivery callback for ack tracking
        producer_->set_delivery_callback([this](const std::string& topic,
                                                 int32_t partition,
                                                 int64_t offset,
                                                 const std::string& error) {
            if (error.empty()) {
                // Successful delivery - we'd need message context to generate proper ack
                // For now, just update stats
                std::lock_guard<std::mutex> lock(stats_mutex_);
                // stats tracked via produce call
            } else {
                LOG(WARNING) << "Delivery failed: " << error;
                std::lock_guard<std::mutex> lock(stats_mutex_);
                stats_.messages_failed++;
            }
        });

        // Create Kafka consumer for v2c messages
        KafkaConsumerConfig v2c_config;
        v2c_config.brokers = config_.kafka_broker;
        v2c_config.group_id = config_.kafka_group_id + "-v2c-" + std::to_string(config_.content_id);
        v2c_config.client_id = "offboard-v2c-" + std::to_string(config_.content_id);
        v2c_consumer_ = std::make_unique<KafkaConsumer>(v2c_config);

        if (!v2c_consumer_->subscribe({v2c_topic_})) {
            LOG(ERROR) << "Failed to subscribe to " << v2c_topic_;
            return false;
        }

        // Create Kafka consumer for status messages
        KafkaConsumerConfig status_config;
        status_config.brokers = config_.kafka_broker;
        status_config.group_id = config_.kafka_group_id + "-status-" + std::to_string(config_.content_id);
        status_config.client_id = "offboard-status-" + std::to_string(config_.content_id);
        status_consumer_ = std::make_unique<KafkaConsumer>(status_config);

        if (!status_consumer_->subscribe({status_topic_})) {
            LOG(ERROR) << "Failed to subscribe to " << status_topic_;
            return false;
        }

        running_ = true;

        // Start consumer threads
        consumer_thread_ = std::thread(&KafkaMqttTransport::ConsumerThread, this);
        status_thread_ = std::thread(&KafkaMqttTransport::StatusConsumerThread, this);

        LOG(INFO) << "KafkaMqttTransport started";
        return true;

    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start KafkaMqttTransport: " << e.what();
        return false;
    }
}

void KafkaMqttTransport::Stop() {
    if (!running_) {
        return;
    }

    LOG(INFO) << "Stopping KafkaMqttTransport...";
    running_ = false;

    // Stop consumers
    if (v2c_consumer_) {
        v2c_consumer_->stop();
    }
    if (status_consumer_) {
        status_consumer_->stop();
    }

    // Wait for threads
    if (consumer_thread_.joinable()) {
        consumer_thread_.join();
    }
    if (status_thread_.joinable()) {
        status_thread_.join();
    }

    // Flush producer
    if (producer_) {
        producer_->flush(5000);
    }

    LOG(INFO) << "KafkaMqttTransport stopped";
}

// =============================================================================
// Methods
// =============================================================================

SendResponse KafkaMqttTransport::SendToVehicle(const SendRequest& request) {
    SendResponse response;

    // Check if vehicle belongs to this partition
    if (!IsVehicleInPartition(request.vehicle_id)) {
        response.status = PublishStatus::WRONG_PARTITION;
        return response;
    }

    // Get/create vehicle state and assign sequence number
    uint64_t sequence;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& state = GetOrCreateVehicleState(request.vehicle_id);
        sequence = state.next_sequence++;
        state.pending_messages++;
    }

    // Check payload size
    if (request.payload.size() > config_.max_payload_size) {
        response.status = PublishStatus::MESSAGE_TOO_LONG;
        return response;
    }

    // Produce to Kafka c2v topic
    // Key is vehicle_id for partitioning
    bool success = producer_->produce(
        c2v_topic_,
        request.vehicle_id,
        request.payload.data(),
        request.payload.size());

    if (!success) {
        response.status = PublishStatus::QUEUE_FULL;
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.messages_failed++;
    } else {
        response.status = PublishStatus::OK;
        response.sequence = sequence;

        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.messages_sent++;
        stats_.bytes_sent += request.payload.size();
    }

    // Determine queue level
    int queue_len = producer_->queue_length();
    if (queue_len == 0) {
        response.queue_level = QueueLevel::EMPTY;
    } else if (queue_len < static_cast<int>(config_.max_queue_size * 0.25)) {
        response.queue_level = QueueLevel::LOW;
    } else if (queue_len < static_cast<int>(config_.max_queue_size * 0.5)) {
        response.queue_level = QueueLevel::NORMAL;
    } else if (queue_len < static_cast<int>(config_.max_queue_size * 0.75)) {
        response.queue_level = QueueLevel::HIGH;
    } else if (queue_len < static_cast<int>(config_.max_queue_size * 0.9)) {
        response.queue_level = QueueLevel::CRITICAL;
    } else {
        response.queue_level = QueueLevel::FULL;
    }

    // Poll for delivery reports
    producer_->poll(0);

    return response;
}

VehicleStatus KafkaMqttTransport::GetVehicleStatus(const std::string& vehicle_id) const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = vehicle_states_.find(vehicle_id);
    if (it == vehicle_states_.end()) {
        return VehicleStatus::UNKNOWN;
    }
    return it->second.status;
}

int64_t KafkaMqttTransport::GetVehicleLastSeen(const std::string& vehicle_id) const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = vehicle_states_.find(vehicle_id);
    if (it == vehicle_states_.end()) {
        return 0;
    }
    return it->second.last_seen_ms;
}

ChannelInfo KafkaMqttTransport::GetChannelInfo() const {
    ChannelInfo info;
    info.content_id = config_.content_id;
    info.partition_id = config_.partition_id;
    info.total_partitions = config_.total_partitions;
    return info;
}

QueueStatusInfo KafkaMqttTransport::GetQueueStatus(const std::string& vehicle_id) const {
    QueueStatusInfo status;
    status.vehicle_id = vehicle_id;
    status.queue_capacity = config_.max_queue_size;

    // For now, we don't track per-vehicle queues, just global queue
    int queue_len = producer_ ? producer_->queue_length() : 0;
    status.queue_size = static_cast<uint32_t>(queue_len);

    if (queue_len == 0) {
        status.level = QueueLevel::EMPTY;
    } else if (queue_len < static_cast<int>(config_.max_queue_size * 0.5)) {
        status.level = QueueLevel::NORMAL;
    } else if (queue_len < static_cast<int>(config_.max_queue_size * 0.75)) {
        status.level = QueueLevel::HIGH;
    } else {
        status.level = QueueLevel::CRITICAL;
    }

    return status;
}

TransportStats KafkaMqttTransport::GetStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    TransportStats result = stats_;

    // Count online vehicles
    std::lock_guard<std::mutex> state_lock(state_mutex_);
    result.vehicles_total = static_cast<uint32_t>(vehicle_states_.size());
    result.vehicles_online = 0;
    for (const auto& [id, state] : vehicle_states_) {
        if (state.status == VehicleStatus::ONLINE) {
            result.vehicles_online++;
        }
    }

    return result;
}

bool KafkaMqttTransport::IsHealthy() const {
    return running_ && producer_ != nullptr;
}

// =============================================================================
// Callbacks
// =============================================================================

void KafkaMqttTransport::SetOnVehicleMessage(VehicleMessageCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    on_message_cb_ = std::move(callback);
}

void KafkaMqttTransport::SetOnAck(AckCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    on_ack_cb_ = std::move(callback);
}

void KafkaMqttTransport::SetOnVehicleStatus(VehicleStatusCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    on_status_cb_ = std::move(callback);
}

void KafkaMqttTransport::SetOnQueueStatusChanged(QueueStatusCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    on_queue_cb_ = std::move(callback);
}

// =============================================================================
// Consumer Threads
// =============================================================================

void KafkaMqttTransport::ConsumerThread() {
    LOG(INFO) << "V2C consumer thread started for " << v2c_topic_;

    while (running_) {
        KafkaMessage msg;
        if (v2c_consumer_->poll(100, msg)) {
            HandleV2CMessage(msg);
            v2c_consumer_->commit(msg);
        }
    }

    LOG(INFO) << "V2C consumer thread stopped";
}

void KafkaMqttTransport::StatusConsumerThread() {
    LOG(INFO) << "Status consumer thread started for " << status_topic_;

    while (running_) {
        KafkaMessage msg;
        if (status_consumer_->poll(100, msg)) {
            HandleStatusMessage(msg);
            status_consumer_->commit(msg);
        }
    }

    LOG(INFO) << "Status consumer thread stopped";
}

// =============================================================================
// Message Handlers
// =============================================================================

void KafkaMqttTransport::HandleV2CMessage(const KafkaMessage& kafka_msg) {
    // Key is vehicle_id
    const std::string& vehicle_id = kafka_msg.key;

    // Check partition
    if (!IsVehicleInPartition(vehicle_id)) {
        return;  // Not for this partition
    }

    // Update vehicle state
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& state = GetOrCreateVehicleState(vehicle_id);
        state.last_seen_ms = CurrentTimeMs();
        if (state.status != VehicleStatus::ONLINE) {
            state.status = VehicleStatus::ONLINE;
            // Fire status callback
            VehicleStatusCallback cb;
            {
                std::lock_guard<std::mutex> cb_lock(callback_mutex_);
                cb = on_status_cb_;
            }
            if (cb) {
                VehicleStatusEvent event;
                event.vehicle_id = vehicle_id;
                event.status = VehicleStatus::ONLINE;
                event.timestamp_ms = state.last_seen_ms;
                event.last_seen_ms = state.last_seen_ms;
                cb(event);
            }
        }
    }

    // Update stats
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        stats_.messages_received++;
        stats_.bytes_received += kafka_msg.value.size();
    }

    // Build VehicleMessage
    VehicleMessage msg;
    msg.vehicle_id = vehicle_id;
    msg.payload = std::vector<uint8_t>(kafka_msg.value.begin(), kafka_msg.value.end());
    msg.sequence = static_cast<uint64_t>(kafka_msg.offset);  // Use Kafka offset as sequence
    msg.timestamp_ms = kafka_msg.timestamp;

    TransportMetadata metadata;
    metadata.source_topic = kafka_msg.topic;
    metadata.received_at_ms = CurrentTimeMs();
    msg.metadata = metadata;

    // Fire callback
    VehicleMessageCallback cb;
    {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        cb = on_message_cb_;
    }
    if (cb) {
        cb(msg);
    }
}

void KafkaMqttTransport::HandleStatusMessage(const KafkaMessage& kafka_msg) {
    // Key is vehicle_id
    const std::string& vehicle_id = kafka_msg.key;

    // Check partition
    if (!IsVehicleInPartition(vehicle_id)) {
        return;  // Not for this partition
    }

    // Value is "1" (online) or "0" (offline)
    bool is_online = (kafka_msg.value == "1");

    VehicleStatus new_status = is_online ? VehicleStatus::ONLINE : VehicleStatus::OFFLINE;
    VehicleStatus old_status;

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto& state = GetOrCreateVehicleState(vehicle_id);
        old_status = state.status;
        state.status = new_status;
        if (is_online) {
            state.last_seen_ms = CurrentTimeMs();
        }
    }

    // Fire callback if status changed
    if (old_status != new_status) {
        VehicleStatusCallback cb;
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            cb = on_status_cb_;
        }
        if (cb) {
            VehicleStatusEvent event;
            event.vehicle_id = vehicle_id;
            event.status = new_status;
            event.timestamp_ms = CurrentTimeMs();
            cb(event);
        }
    }
}

// =============================================================================
// Helper Methods
// =============================================================================

KafkaMqttTransport::VehicleState& KafkaMqttTransport::GetOrCreateVehicleState(
    const std::string& vehicle_id) {
    auto it = vehicle_states_.find(vehicle_id);
    if (it == vehicle_states_.end()) {
        it = vehicle_states_.emplace(vehicle_id, VehicleState{}).first;
    }
    return it->second;
}

bool KafkaMqttTransport::IsVehicleInPartition(const std::string& vehicle_id) const {
    if (config_.total_partitions <= 1) {
        return true;  // Non-partitioned, accept all
    }
    return ComputePartition(vehicle_id) == config_.partition_id;
}

uint32_t KafkaMqttTransport::ComputePartition(const std::string& vehicle_id) const {
    // Simple hash-based partitioning
    std::hash<std::string> hasher;
    return static_cast<uint32_t>(hasher(vehicle_id) % config_.total_partitions);
}

int64_t KafkaMqttTransport::CurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// =============================================================================
// Factory Function
// =============================================================================

std::unique_ptr<OffboardTransport> CreateKafkaMqttTransport(
    const OffboardTransportConfig& config) {
    return std::make_unique<KafkaMqttTransport>(config);
}

}  // namespace ifex::offboard
