#include "cloud_backend_transport_server.hpp"
#include "kafka_mqtt_transport.hpp"

#include <glog/logging.h>

namespace ifex::offboard {

CloudBackendTransportServer::CloudBackendTransportServer(const Config& config)
    : config_(config) {

    // Build OffboardTransportConfig from server config
    OffboardTransportConfig transport_config;
    transport_config.content_id = config_.content_id;
    transport_config.partition_id = config_.partition_id;
    transport_config.total_partitions = config_.total_partitions;
    transport_config.kafka_broker = config_.kafka_broker;
    transport_config.kafka_group_id = config_.kafka_group_id;
    transport_config.kafka_topic_v2c = config_.kafka_topic_v2c;
    transport_config.kafka_topic_c2v = config_.kafka_topic_c2v;
    transport_config.kafka_topic_status = config_.kafka_topic_status;

    transport_ = CreateKafkaMqttTransport(transport_config);

    LOG(INFO) << "CloudBackendTransportServer created for content_id=" << config_.content_id;
}

CloudBackendTransportServer::~CloudBackendTransportServer() {
    Stop();
}

bool CloudBackendTransportServer::Start() {
    if (!transport_) {
        LOG(ERROR) << "Transport not initialized";
        return false;
    }

    // Set up callbacks
    transport_->SetOnVehicleMessage([this](const VehicleMessage& msg) {
        OnVehicleMessage(msg);
    });

    transport_->SetOnVehicleStatus([this](const VehicleStatusEvent& event) {
        OnVehicleStatus(event);
    });

    transport_->SetOnAck([this](const DeliveryAck& ack) {
        OnAck(ack);
    });

    transport_->SetOnQueueStatusChanged([this](const QueueStatusInfo& status) {
        OnQueueStatusChanged(status);
    });

    return transport_->Start();
}

void CloudBackendTransportServer::Stop() {
    if (transport_) {
        transport_->Stop();
    }
}

bool CloudBackendTransportServer::IsRunning() const {
    return transport_ && transport_->IsRunning() && transport_->IsHealthy();
}

// =============================================================================
// gRPC Method Implementations
// =============================================================================

grpc::Status CloudBackendTransportServer::send_to_vehicle(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::send_to_vehicle_request* request,
    swdv::cloud_backend_transport_service::send_to_vehicle_response* response) {

    if (!transport_) {
        return grpc::Status(grpc::UNAVAILABLE, "Transport not initialized");
    }

    const auto& req = request->request();

    SendRequest send_req;
    send_req.vehicle_id = req.vehicle_id();
    send_req.payload = std::vector<uint8_t>(req.payload().begin(), req.payload().end());
    send_req.persistence = static_cast<Persistence>(req.persistence());

    SendResponse send_resp = transport_->SendToVehicle(send_req);

    auto* result = response->mutable_result();
    result->set_sequence(send_resp.sequence);
    result->set_status(static_cast<swdv::cloud_backend_transport_service::publish_status_t>(send_resp.status));
    result->set_queue_level(static_cast<swdv::cloud_backend_transport_service::queue_level_t>(send_resp.queue_level));

    return grpc::Status::OK;
}

grpc::Status CloudBackendTransportServer::get_vehicle_status(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::get_vehicle_status_request* request,
    swdv::cloud_backend_transport_service::get_vehicle_status_response* response) {

    if (!transport_) {
        return grpc::Status(grpc::UNAVAILABLE, "Transport not initialized");
    }

    VehicleStatus status = transport_->GetVehicleStatus(request->vehicle_id());
    int64_t last_seen = transport_->GetVehicleLastSeen(request->vehicle_id());

    response->set_status(static_cast<swdv::cloud_backend_transport_service::vehicle_status_t>(status));
    response->set_last_seen_ms(last_seen);

    return grpc::Status::OK;
}

grpc::Status CloudBackendTransportServer::get_channel_info(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::get_channel_info_request* request,
    swdv::cloud_backend_transport_service::get_channel_info_response* response) {

    if (!transport_) {
        return grpc::Status(grpc::UNAVAILABLE, "Transport not initialized");
    }

    ChannelInfo info = transport_->GetChannelInfo();

    auto* result = response->mutable_info();
    result->set_content_id(info.content_id);
    result->set_partition_id(info.partition_id);
    result->set_total_partitions(info.total_partitions);

    return grpc::Status::OK;
}

grpc::Status CloudBackendTransportServer::get_queue_status(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::get_queue_status_request* request,
    swdv::cloud_backend_transport_service::get_queue_status_response* response) {

    if (!transport_) {
        return grpc::Status(grpc::UNAVAILABLE, "Transport not initialized");
    }

    QueueStatusInfo status = transport_->GetQueueStatus(request->vehicle_id());

    auto* result = response->mutable_status();
    result->set_vehicle_id(status.vehicle_id);
    result->set_level(static_cast<swdv::cloud_backend_transport_service::queue_level_t>(status.level));
    result->set_queue_size(status.queue_size);
    result->set_queue_capacity(status.queue_capacity);

    return grpc::Status::OK;
}

grpc::Status CloudBackendTransportServer::get_stats(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::get_stats_request* request,
    swdv::cloud_backend_transport_service::get_stats_response* response) {

    if (!transport_) {
        return grpc::Status(grpc::UNAVAILABLE, "Transport not initialized");
    }

    TransportStats stats = transport_->GetStats();

    auto* result = response->mutable_stats();
    result->set_messages_sent(stats.messages_sent);
    result->set_messages_failed(stats.messages_failed);
    result->set_bytes_sent(stats.bytes_sent);
    result->set_messages_received(stats.messages_received);
    result->set_bytes_received(stats.bytes_received);
    result->set_vehicles_online(stats.vehicles_online);
    result->set_vehicles_total(stats.vehicles_total);

    return grpc::Status::OK;
}

grpc::Status CloudBackendTransportServer::healthy(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::healthy_request* request,
    swdv::cloud_backend_transport_service::healthy_response* response) {

    response->set_is_healthy(transport_ && transport_->IsHealthy());
    return grpc::Status::OK;
}

// =============================================================================
// gRPC Streaming Event Implementations
// =============================================================================

grpc::Status CloudBackendTransportServer::subscribe(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::on_vehicle_message_subscribe_request* request,
    grpc::ServerWriter<swdv::cloud_backend_transport_service::on_vehicle_message>* writer) {

    // Register this writer
    {
        std::unique_lock lock(message_streams_mutex_);
        message_streams_.push_back(writer);
    }

    // Block until client disconnects
    while (!context->IsCancelled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Unregister
    {
        std::unique_lock lock(message_streams_mutex_);
        message_streams_.erase(
            std::remove(message_streams_.begin(), message_streams_.end(), writer),
            message_streams_.end());
    }

    return grpc::Status::OK;
}

grpc::Status CloudBackendTransportServer::subscribe(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::on_ack_subscribe_request* request,
    grpc::ServerWriter<swdv::cloud_backend_transport_service::on_ack>* writer) {

    {
        std::unique_lock lock(ack_streams_mutex_);
        ack_streams_.push_back(writer);
    }

    while (!context->IsCancelled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    {
        std::unique_lock lock(ack_streams_mutex_);
        ack_streams_.erase(
            std::remove(ack_streams_.begin(), ack_streams_.end(), writer),
            ack_streams_.end());
    }

    return grpc::Status::OK;
}

grpc::Status CloudBackendTransportServer::subscribe(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::on_vehicle_status_subscribe_request* request,
    grpc::ServerWriter<swdv::cloud_backend_transport_service::on_vehicle_status>* writer) {

    {
        std::unique_lock lock(status_streams_mutex_);
        status_streams_.push_back(writer);
    }

    while (!context->IsCancelled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    {
        std::unique_lock lock(status_streams_mutex_);
        status_streams_.erase(
            std::remove(status_streams_.begin(), status_streams_.end(), writer),
            status_streams_.end());
    }

    return grpc::Status::OK;
}

grpc::Status CloudBackendTransportServer::subscribe(
    grpc::ServerContext* context,
    const swdv::cloud_backend_transport_service::on_queue_status_changed_subscribe_request* request,
    grpc::ServerWriter<swdv::cloud_backend_transport_service::on_queue_status_changed>* writer) {

    {
        std::unique_lock lock(queue_streams_mutex_);
        queue_streams_.push_back(writer);
    }

    while (!context->IsCancelled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    {
        std::unique_lock lock(queue_streams_mutex_);
        queue_streams_.erase(
            std::remove(queue_streams_.begin(), queue_streams_.end(), writer),
            queue_streams_.end());
    }

    return grpc::Status::OK;
}

// =============================================================================
// Transport Callbacks
// =============================================================================

void CloudBackendTransportServer::OnVehicleMessage(const VehicleMessage& msg) {
    BroadcastVehicleMessage(msg);
}

void CloudBackendTransportServer::OnVehicleStatus(const VehicleStatusEvent& event) {
    BroadcastVehicleStatus(event);
}

void CloudBackendTransportServer::OnAck(const DeliveryAck& ack) {
    BroadcastAck(ack);
}

void CloudBackendTransportServer::OnQueueStatusChanged(const QueueStatusInfo& status) {
    BroadcastQueueStatus(status);
}

// =============================================================================
// Broadcast to gRPC Streams
// =============================================================================

void CloudBackendTransportServer::BroadcastVehicleMessage(const VehicleMessage& msg) {
    swdv::cloud_backend_transport_service::on_vehicle_message event;
    auto* vm = event.mutable_message();
    vm->set_vehicle_id(msg.vehicle_id);
    vm->set_payload(msg.payload.data(), msg.payload.size());
    vm->set_sequence(msg.sequence);
    vm->set_timestamp_ms(msg.timestamp_ms);

    if (msg.metadata.has_value()) {
        auto* meta = vm->mutable_metadata();
        meta->set_originator(msg.metadata->originator);
        meta->set_message_id(msg.metadata->message_id);
        meta->set_received_at_ms(msg.metadata->received_at_ms);
        meta->set_source_topic(msg.metadata->source_topic);
    }

    std::shared_lock lock(message_streams_mutex_);
    for (auto* writer : message_streams_) {
        writer->Write(event);
    }
}

void CloudBackendTransportServer::BroadcastVehicleStatus(const VehicleStatusEvent& event) {
    swdv::cloud_backend_transport_service::on_vehicle_status grpc_event;
    auto* vs = grpc_event.mutable_event();
    vs->set_vehicle_id(event.vehicle_id);
    vs->set_status(static_cast<swdv::cloud_backend_transport_service::vehicle_status_t>(event.status));
    vs->set_timestamp_ms(event.timestamp_ms);
    vs->set_last_seen_ms(event.last_seen_ms);

    std::shared_lock lock(status_streams_mutex_);
    for (auto* writer : status_streams_) {
        writer->Write(grpc_event);
    }
}

void CloudBackendTransportServer::BroadcastAck(const DeliveryAck& ack) {
    swdv::cloud_backend_transport_service::on_ack event;
    auto* da = event.mutable_ack();
    da->set_vehicle_id(ack.vehicle_id);
    da->set_sequence(ack.sequence);

    std::shared_lock lock(ack_streams_mutex_);
    for (auto* writer : ack_streams_) {
        writer->Write(event);
    }
}

void CloudBackendTransportServer::BroadcastQueueStatus(const QueueStatusInfo& status) {
    swdv::cloud_backend_transport_service::on_queue_status_changed event;
    auto* qs = event.mutable_status();
    qs->set_vehicle_id(status.vehicle_id);
    qs->set_level(static_cast<swdv::cloud_backend_transport_service::queue_level_t>(status.level));
    qs->set_queue_size(status.queue_size);
    qs->set_queue_capacity(status.queue_capacity);

    std::shared_lock lock(queue_streams_mutex_);
    for (auto* writer : queue_streams_) {
        writer->Write(event);
    }
}

}  // namespace ifex::offboard
