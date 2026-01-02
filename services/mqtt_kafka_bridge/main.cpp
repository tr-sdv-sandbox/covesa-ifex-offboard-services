/// MQTT-Kafka Bridge Service
///
/// Routes MQTT messages from vehicles to Kafka topics based on content_id.
/// Uses the mqtt_kafka_router library for clean separation of routing logic
/// and content-specific transformations.
///
/// Transforms vehicle protos to offboard protos:
/// - Adds vehicle_id (from MQTT topic, verified by ACL)
/// - Adds enrichment data (fleet_id, region from VehicleContext)
/// - Adds offboard timestamps

#include <csignal>
#include <memory>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "mqtt_kafka_router.hpp"

// Vehicle-side codecs (decode incoming)
#include "discovery_codec.hpp"
#include "scheduler_codec.hpp"
#include "rpc_codec.hpp"

// Offboard codecs (encode outgoing)
#include "discovery_offboard_codec.hpp"
#include "scheduler_offboard_codec.hpp"
#include "rpc_offboard_codec.hpp"

// MQTT flags
DEFINE_string(mqtt_host, "localhost", "MQTT broker hostname");
DEFINE_int32(mqtt_port, 1883, "MQTT broker port");
DEFINE_string(mqtt_client_id, "mqtt-kafka-bridge", "MQTT client ID");
DEFINE_string(mqtt_username, "", "MQTT username (optional)");
DEFINE_string(mqtt_password, "", "MQTT password (optional)");
DEFINE_string(mqtt_topic, "v2c/#", "MQTT topic pattern to subscribe to");

// Kafka flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_client_id, "mqtt-kafka-bridge", "Kafka client ID");

// Topic configuration
DEFINE_string(kafka_topic_rpc, "ifex.rpc.200", "Kafka topic for RPC (content_id=200)");
DEFINE_string(kafka_topic_discovery, "ifex.discovery.201", "Kafka topic for discovery (content_id=201)");
DEFINE_string(kafka_topic_scheduler, "ifex.scheduler.202", "Kafka topic for scheduler (content_id=202)");

// Behavior flags
DEFINE_bool(require_vehicle_context, false, "Require vehicle to be known (drop unknown)");

// Bridge identification
DEFINE_string(bridge_id, "mqtt-kafka-bridge-1", "Bridge instance ID for tracking");

// Global router instance for signal handling
ifex::offboard::MqttKafkaRouter* g_router = nullptr;

void signal_handler(int signum) {
    LOG(INFO) << "Received signal " << signum << ", shutting down...";
    if (g_router) {
        g_router->stop();
    }
}

/// Create a transform for discovery sync messages (content_id=201)
/// Decodes vehicle proto, wraps in offboard envelope with metadata
ifex::offboard::TransformFn make_discovery_transform(const std::string& bridge_id) {
    return [bridge_id](const std::string& vehicle_id,
              uint32_t content_id,
              const std::string& payload,
              const std::optional<ifex::offboard::VehicleContext>& ctx)
        -> std::optional<std::string> {

        // Decode the incoming sync message from vehicle
        auto msg = ifex::offboard::decode_discovery_sync(payload);
        if (!msg) {
            LOG(WARNING) << "Failed to decode discovery sync from " << vehicle_id;
            return std::nullopt;  // Drop malformed messages
        }

        // Extract enrichment from context
        std::string fleet_id, region;
        if (ctx) {
            fleet_id = ctx->fleet_id;
            region = ctx->region;
        }

        // Create offboard envelope with vehicle_id and enrichment
        std::string offboard_payload = ifex::offboard::encode_discovery_offboard(
            vehicle_id,
            *msg,
            fleet_id,
            region,
            bridge_id
        );

        LOG(INFO) << "Discovery sync from " << vehicle_id
                  << ": events=" << msg->events_size()
                  << " total_services=" << msg->total_services()
                  << " fleet=" << fleet_id
                  << " -> offboard(" << offboard_payload.size() << " bytes)";

        return offboard_payload;
    };
}

/// Create a transform for scheduler sync messages (content_id=202)
/// Decodes vehicle proto, wraps in offboard envelope with metadata
ifex::offboard::TransformFn make_scheduler_transform(const std::string& bridge_id) {
    return [bridge_id](const std::string& vehicle_id,
              uint32_t content_id,
              const std::string& payload,
              const std::optional<ifex::offboard::VehicleContext>& ctx)
        -> std::optional<std::string> {

        // Decode the incoming sync message from vehicle
        auto msg = ifex::offboard::decode_scheduler_sync(payload);
        if (!msg) {
            LOG(WARNING) << "Failed to decode scheduler sync from " << vehicle_id;
            return std::nullopt;
        }

        // Extract enrichment from context
        std::string fleet_id, region;
        if (ctx) {
            fleet_id = ctx->fleet_id;
            region = ctx->region;
        }

        // Create offboard envelope with vehicle_id and enrichment
        std::string offboard_payload = ifex::offboard::encode_scheduler_offboard(
            vehicle_id,
            *msg,
            fleet_id,
            region,
            bridge_id
        );

        LOG(INFO) << "Scheduler sync from " << vehicle_id
                  << ": events=" << msg->events_size()
                  << " active_jobs=" << msg->active_jobs_count()
                  << " fleet=" << fleet_id
                  << " -> offboard(" << offboard_payload.size() << " bytes)";

        return offboard_payload;
    };
}

/// Create a transform for RPC messages (content_id=200)
/// Decodes vehicle proto (response), wraps in offboard envelope with metadata
ifex::offboard::TransformFn make_rpc_transform(const std::string& bridge_id) {
    return [bridge_id](const std::string& vehicle_id,
              uint32_t content_id,
              const std::string& payload,
              const std::optional<ifex::offboard::VehicleContext>& ctx)
        -> std::optional<std::string> {

        // Extract enrichment from context
        std::string fleet_id, region;
        if (ctx) {
            fleet_id = ctx->fleet_id;
            region = ctx->region;
        }

        // Try to decode as response first (v2c direction is typically responses)
        auto response = ifex::offboard::decode_rpc_response(payload);
        if (response) {
            // Create offboard envelope for response
            std::string offboard_payload = ifex::offboard::encode_rpc_response_offboard(
                vehicle_id,
                *response,
                fleet_id,
                region,
                bridge_id
            );

            LOG(INFO) << "RPC response from " << vehicle_id
                      << ": correlation_id=" << response->correlation_id()
                      << " status=" << ifex::offboard::rpc_status_name(response->status())
                      << " fleet=" << fleet_id
                      << " -> offboard(" << offboard_payload.size() << " bytes)";

            return offboard_payload;
        }

        // Also try request (for logging/debugging unexpected messages on v2c)
        auto request = ifex::offboard::decode_rpc_request(payload);
        if (request) {
            LOG(WARNING) << "RPC request on v2c (unexpected) from " << vehicle_id
                         << ": correlation_id=" << request->correlation_id()
                         << " - forwarding as-is";
            // Forward requests as-is (unexpected on v2c direction)
            return payload;
        }

        LOG(WARNING) << "Failed to decode RPC message from " << vehicle_id;
        return std::nullopt;
    };
}

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    LOG(INFO) << "MQTT-Kafka Bridge starting...";
    LOG(INFO) << "MQTT: " << FLAGS_mqtt_host << ":" << FLAGS_mqtt_port;
    LOG(INFO) << "Kafka: " << FLAGS_kafka_broker;
    LOG(INFO) << "Bridge ID: " << FLAGS_bridge_id;

    // Configure MQTT
    ifex::offboard::MqttRouterConfig mqtt_config;
    mqtt_config.host = FLAGS_mqtt_host;
    mqtt_config.port = FLAGS_mqtt_port;
    mqtt_config.client_id = FLAGS_mqtt_client_id;
    mqtt_config.username = FLAGS_mqtt_username;
    mqtt_config.password = FLAGS_mqtt_password;
    mqtt_config.topic_pattern = FLAGS_mqtt_topic;

    // Configure Kafka
    ifex::offboard::KafkaRouterConfig kafka_config;
    kafka_config.brokers = FLAGS_kafka_broker;
    kafka_config.client_id = FLAGS_kafka_client_id;

    // Create router
    ifex::offboard::MqttKafkaRouter router(mqtt_config, kafka_config);
    g_router = &router;

    // Create vehicle context store (for enrichment)
    auto context_store = std::make_shared<ifex::offboard::VehicleContextStore>();
    router.set_context_store(context_store);
    router.set_require_context(FLAGS_require_vehicle_context);

    // Register handlers for each content_id
    // All messages are transformed to offboard format with vehicle_id and enrichment
    router.register_handler(200, FLAGS_kafka_topic_rpc,
                            make_rpc_transform(FLAGS_bridge_id));
    router.register_handler(201, FLAGS_kafka_topic_discovery,
                            make_discovery_transform(FLAGS_bridge_id));
    router.register_handler(202, FLAGS_kafka_topic_scheduler,
                            make_scheduler_transform(FLAGS_bridge_id));

    LOG(INFO) << "Registered handlers:";
    LOG(INFO) << "  200 -> " << FLAGS_kafka_topic_rpc;
    LOG(INFO) << "  201 -> " << FLAGS_kafka_topic_discovery;
    LOG(INFO) << "  202 -> " << FLAGS_kafka_topic_scheduler;

    // Run the router (blocking)
    router.run();

    // Print final stats
    const auto& stats = router.stats();
    LOG(INFO) << "Final stats:";
    LOG(INFO) << "  messages_received=" << stats.messages_received;
    LOG(INFO) << "  messages_transformed=" << stats.messages_transformed;
    LOG(INFO) << "  messages_produced=" << stats.messages_produced;
    LOG(INFO) << "  messages_dropped=" << stats.messages_dropped;
    LOG(INFO) << "  transform_errors=" << stats.transform_errors;
    LOG(INFO) << "  produce_errors=" << stats.produce_errors;

    LOG(INFO) << "Goodbye!";
    return 0;
}
