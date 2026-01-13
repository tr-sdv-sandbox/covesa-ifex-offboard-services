#pragma once

/// @file cloud_backend_transport_server.hpp
/// @brief Kafka+MQTT-based CloudBackendTransportServer for production use.
///
/// This implements the same gRPC interface as ifex-core's CloudBackendTransportServer
/// but uses OffboardTransport (Kafka+MQTT) as the underlying transport.
///
/// Architecture:
///   CloudBackendTransportClient (gRPC, from ifex-core)
///       ↓
///   CloudBackendTransportServer (this class, gRPC)
///       ↓
///   OffboardTransport (KafkaMqttTransport)
///       ↓
///   mqtt_kafka_bridge → MQTT → Vehicle

#include "cloud-backend-transport-service.grpc.pb.h"
#include "offboard_transport.hpp"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace ifex::offboard {

/// Kafka+MQTT based cloud backend transport server.
///
/// Implements the IFEX cloud_backend_transport_service gRPC API using
/// OffboardTransport for actual message delivery.
class CloudBackendTransportServer final
    : public swdv::cloud_backend_transport_service::send_to_vehicle_service::Service,
      public swdv::cloud_backend_transport_service::get_vehicle_status_service::Service,
      public swdv::cloud_backend_transport_service::get_channel_info_service::Service,
      public swdv::cloud_backend_transport_service::get_queue_status_service::Service,
      public swdv::cloud_backend_transport_service::get_stats_service::Service,
      public swdv::cloud_backend_transport_service::healthy_service::Service,
      public swdv::cloud_backend_transport_service::on_vehicle_message_service::Service,
      public swdv::cloud_backend_transport_service::on_ack_service::Service,
      public swdv::cloud_backend_transport_service::on_vehicle_status_service::Service,
      public swdv::cloud_backend_transport_service::on_queue_status_changed_service::Service {
public:
    struct Config {
        // Kafka settings
        std::string kafka_broker = "localhost:9092";
        std::string kafka_group_id = "cloud-backend-transport";

        // Topic names (must match mqtt_kafka_bridge config)
        std::string kafka_topic_v2c;        ///< e.g., "ifex.scheduler.202"
        std::string kafka_topic_c2v;        ///< e.g., "ifex.c2v.scheduler"
        std::string kafka_topic_status = "ifex.status";

        // Channel binding
        uint32_t content_id = 200;
        uint32_t partition_id = 0;
        uint32_t total_partitions = 1;
    };

    explicit CloudBackendTransportServer(const Config& config);
    ~CloudBackendTransportServer();

    // Non-copyable
    CloudBackendTransportServer(const CloudBackendTransportServer&) = delete;
    CloudBackendTransportServer& operator=(const CloudBackendTransportServer&) = delete;

    /// Start the transport
    bool Start();

    /// Stop the transport
    void Stop();

    /// Check if running and healthy
    bool IsRunning() const;

    // =========================================================================
    // gRPC Method Implementations
    // =========================================================================

    grpc::Status send_to_vehicle(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::send_to_vehicle_request* request,
        swdv::cloud_backend_transport_service::send_to_vehicle_response* response) override;

    grpc::Status get_vehicle_status(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::get_vehicle_status_request* request,
        swdv::cloud_backend_transport_service::get_vehicle_status_response* response) override;

    grpc::Status get_channel_info(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::get_channel_info_request* request,
        swdv::cloud_backend_transport_service::get_channel_info_response* response) override;

    grpc::Status get_queue_status(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::get_queue_status_request* request,
        swdv::cloud_backend_transport_service::get_queue_status_response* response) override;

    grpc::Status get_stats(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::get_stats_request* request,
        swdv::cloud_backend_transport_service::get_stats_response* response) override;

    grpc::Status healthy(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::healthy_request* request,
        swdv::cloud_backend_transport_service::healthy_response* response) override;

    // =========================================================================
    // gRPC Streaming Event Implementations
    // =========================================================================

    grpc::Status subscribe(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::on_vehicle_message_subscribe_request* request,
        grpc::ServerWriter<swdv::cloud_backend_transport_service::on_vehicle_message>* writer) override;

    grpc::Status subscribe(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::on_ack_subscribe_request* request,
        grpc::ServerWriter<swdv::cloud_backend_transport_service::on_ack>* writer) override;

    grpc::Status subscribe(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::on_vehicle_status_subscribe_request* request,
        grpc::ServerWriter<swdv::cloud_backend_transport_service::on_vehicle_status>* writer) override;

    grpc::Status subscribe(
        grpc::ServerContext* context,
        const swdv::cloud_backend_transport_service::on_queue_status_changed_subscribe_request* request,
        grpc::ServerWriter<swdv::cloud_backend_transport_service::on_queue_status_changed>* writer) override;

private:
    // Callbacks from OffboardTransport
    void OnVehicleMessage(const VehicleMessage& msg);
    void OnVehicleStatus(const VehicleStatusEvent& event);
    void OnAck(const DeliveryAck& ack);
    void OnQueueStatusChanged(const QueueStatusInfo& status);

    // Broadcast to gRPC stream subscribers
    void BroadcastVehicleMessage(const VehicleMessage& msg);
    void BroadcastVehicleStatus(const VehicleStatusEvent& event);
    void BroadcastAck(const DeliveryAck& ack);
    void BroadcastQueueStatus(const QueueStatusInfo& status);

    Config config_;
    std::unique_ptr<OffboardTransport> transport_;

    // Stream subscribers (gRPC server-streaming)
    std::shared_mutex message_streams_mutex_;
    std::vector<grpc::ServerWriter<swdv::cloud_backend_transport_service::on_vehicle_message>*> message_streams_;

    std::shared_mutex status_streams_mutex_;
    std::vector<grpc::ServerWriter<swdv::cloud_backend_transport_service::on_vehicle_status>*> status_streams_;

    std::shared_mutex ack_streams_mutex_;
    std::vector<grpc::ServerWriter<swdv::cloud_backend_transport_service::on_ack>*> ack_streams_;

    std::shared_mutex queue_streams_mutex_;
    std::vector<grpc::ServerWriter<swdv::cloud_backend_transport_service::on_queue_status_changed>*> queue_streams_;
};

}  // namespace ifex::offboard
