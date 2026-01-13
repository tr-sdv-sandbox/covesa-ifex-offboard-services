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
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "mqtt_kafka_router.hpp"
#include "enrichment_consumer.hpp"
#include "postgres_client.hpp"

// Vehicle-side codecs (decode incoming)
#include "scheduler_codec.hpp"
#include "rpc_codec.hpp"

// Offboard codecs (encode outgoing)
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

// v2c topic configuration (vehicle-to-cloud)
DEFINE_string(kafka_topic_rpc, "ifex.rpc.200", "Kafka topic for RPC responses (content_id=200)");
DEFINE_string(kafka_topic_discovery, "ifex.discovery.201", "Kafka topic for discovery (content_id=201)");
DEFINE_string(kafka_topic_scheduler, "ifex.scheduler.202", "Kafka topic for scheduler (content_id=202)");

// c2v topic configuration (cloud-to-vehicle)
DEFINE_string(kafka_topic_c2v_rpc, "ifex.c2v.rpc", "Kafka topic for RPC requests to vehicles");
DEFINE_string(kafka_topic_c2v_discovery, "ifex.c2v.discovery", "Kafka topic for discovery schema requests to vehicles");
DEFINE_string(kafka_topic_c2v_scheduler, "ifex.c2v.scheduler", "Kafka topic for scheduler commands to vehicles");
DEFINE_string(kafka_c2v_group_id, "mqtt-kafka-bridge-c2v", "Consumer group ID for c2v topics");

// Enrichment configuration
DEFINE_string(kafka_topic_enrichment, "ifex.vehicle.enrichment", "Kafka topic for vehicle enrichment");
DEFINE_int32(enrichment_load_timeout_s, 30, "Timeout for initial enrichment load (seconds)");

// Behavior flags
DEFINE_bool(require_vehicle_context, false, "Require vehicle to be known (drop unknown)");

// Status tracking
DEFINE_string(kafka_topic_status, "ifex.status", "Kafka topic for vehicle status (is_online)");
DEFINE_int32(heartbeat_timeout_s, 60, "Seconds without message before marking vehicle offline");
DEFINE_int32(heartbeat_check_interval_s, 10, "Interval between heartbeat timeout checks");

// PostgreSQL flags
DEFINE_string(postgres_host, "localhost", "PostgreSQL host");
DEFINE_int32(postgres_port, 5432, "PostgreSQL port");
DEFINE_string(postgres_db, "ifex_offboard", "PostgreSQL database name");
DEFINE_string(postgres_user, "ifex", "PostgreSQL user");
DEFINE_string(postgres_password, "ifex_dev", "PostgreSQL password");

// Bridge identification
DEFINE_string(bridge_id, "mqtt-kafka-bridge-1", "Bridge instance ID for tracking");

// Global router instance for signal handling
ifex::offboard::MqttKafkaRouter* g_router = nullptr;
std::atomic<bool> g_running{true};

void signal_handler(int signum) {
    LOG(INFO) << "Received signal " << signum << ", shutting down...";
    g_running = false;
    if (g_router) {
        g_router->stop();
    }
}

/// Create a transform for scheduler messages (content_id=202)
/// Handles both sync_message_t (state sync) and scheduler_command_ack_t (command acks)
/// Decodes vehicle proto, wraps in offboard envelope with metadata
ifex::offboard::TransformFn make_scheduler_transform(const std::string& bridge_id) {
    return [bridge_id](const std::string& vehicle_id,
              uint32_t content_id,
              const std::string& payload,
              const std::optional<ifex::offboard::VehicleContext>& ctx)
        -> std::optional<std::string> {

        // Try to decode as sync message first (most common case)
        auto msg = ifex::offboard::decode_scheduler_sync(payload);
        if (msg) {
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
        }

        // Try to decode as command ack (sent after cloud command execution)
        auto ack = ifex::offboard::decode_scheduler_command_ack(payload);
        if (ack) {
            // Log the command ack but don't forward to Kafka (drop)
            // Cloud scheduler API tracks its own commands via request correlation
            LOG(INFO) << "Scheduler command ack from " << vehicle_id
                      << ": command_id=" << ack->command_id()
                      << " success=" << (ack->success() ? "true" : "false")
                      << (ack->job_id().empty() ? "" : " job_id=" + ack->job_id())
                      << (ack->error_message().empty() ? "" : " error=" + ack->error_message());

            // Return nullopt to drop the message (don't forward to Kafka)
            return std::nullopt;
        }

        LOG(WARNING) << "Failed to decode scheduler message from " << vehicle_id
                     << " (" << payload.size() << " bytes) - unknown message type";
        return std::nullopt;
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
    kafka_config.c2v_group_id = FLAGS_kafka_c2v_group_id;

    // Create router
    ifex::offboard::MqttKafkaRouter router(mqtt_config, kafka_config);
    g_router = &router;

    // Create vehicle context store (for enrichment)
    auto context_store = std::make_shared<ifex::offboard::VehicleContextStore>();

    // Configure and start enrichment consumer
    ifex::offboard::EnrichmentConsumerConfig enrichment_config;
    enrichment_config.brokers = FLAGS_kafka_broker;
    enrichment_config.topic = FLAGS_kafka_topic_enrichment;
    enrichment_config.initial_load_timeout = std::chrono::seconds(FLAGS_enrichment_load_timeout_s);
    enrichment_config.group_id = FLAGS_kafka_client_id + "-enrichment";
    enrichment_config.client_id = FLAGS_kafka_client_id + "-enrichment";

    ifex::offboard::EnrichmentConsumer enrichment_consumer(enrichment_config, context_store);

    // Load initial enrichment data (blocking)
    LOG(INFO) << "Loading vehicle enrichment from " << FLAGS_kafka_topic_enrichment << "...";
    size_t loaded = enrichment_consumer.load_initial();
    LOG(INFO) << "Loaded " << loaded << " enrichment records for " << context_store->size() << " vehicles";

    // Start background consumer for updates
    enrichment_consumer.start_background();

    // Connect context store to router
    router.set_context_store(context_store);
    router.set_require_context(FLAGS_require_vehicle_context);

    // Register v2c handlers for each content_id (vehicle-to-cloud)
    // RPC messages are transformed to offboard format with vehicle_id
    // Discovery and scheduler use passthrough (v2 protocol includes vehicle_id)
    router.register_handler(200, FLAGS_kafka_topic_rpc,
                            make_rpc_transform(FLAGS_bridge_id));
    router.register_handler(201, FLAGS_kafka_topic_discovery, nullptr);  // Passthrough
    router.register_handler(202, FLAGS_kafka_topic_scheduler, nullptr);  // Passthrough (v2 protocol)

    LOG(INFO) << "Registered v2c handlers (MQTT -> Kafka):";
    LOG(INFO) << "  v2c/*/200 -> " << FLAGS_kafka_topic_rpc;
    LOG(INFO) << "  v2c/*/201 -> " << FLAGS_kafka_topic_discovery;
    LOG(INFO) << "  v2c/*/202 -> " << FLAGS_kafka_topic_scheduler;

    // Register c2v handlers (cloud-to-vehicle)
    // These consume from Kafka and publish to MQTT c2v topics
    // Default transform: Kafka message key = vehicle_id, value = payload (passthrough)
    router.register_c2v_handler(FLAGS_kafka_topic_c2v_rpc, 200);         // RPC requests
    router.register_c2v_handler(FLAGS_kafka_topic_c2v_discovery, 201);   // Schema requests
    router.register_c2v_handler(FLAGS_kafka_topic_c2v_scheduler, 202);   // Scheduler commands

    LOG(INFO) << "Registered c2v handlers (Kafka -> MQTT):";
    LOG(INFO) << "  " << FLAGS_kafka_topic_c2v_rpc << " -> c2v/*/200";
    LOG(INFO) << "  " << FLAGS_kafka_topic_c2v_discovery << " -> c2v/*/201";
    LOG(INFO) << "  " << FLAGS_kafka_topic_c2v_scheduler << " -> c2v/*/202";

    // Initialize PostgreSQL for status updates
    ifex::offboard::PostgresConfig pg_config;
    pg_config.host = FLAGS_postgres_host;
    pg_config.port = FLAGS_postgres_port;
    pg_config.database = FLAGS_postgres_db;
    pg_config.user = FLAGS_postgres_user;
    pg_config.password = FLAGS_postgres_password;

    auto db = std::make_shared<ifex::offboard::PostgresClient>(pg_config);
    if (!db->is_connected()) {
        LOG(WARNING) << "Failed to connect to PostgreSQL - status updates will not be persisted";
    } else {
        LOG(INFO) << "PostgreSQL connected: " << FLAGS_postgres_host << ":" << FLAGS_postgres_port
                  << "/" << FLAGS_postgres_db;
    }

    // Register status handler for vehicle online/offline detection
    // Updates both Kafka and PostgreSQL
    router.set_status_handler(FLAGS_kafka_topic_status,
        [&db](const std::string& vehicle_id, bool is_online) {
            LOG(INFO) << "Vehicle " << vehicle_id << " is_online=" << is_online;

            if (db && db->is_connected()) {
                // Upsert vehicle with online status
                auto result = db->execute(
                    "INSERT INTO vehicles (vehicle_id, is_online, last_seen_at) "
                    "VALUES ($1, $2, NOW()) "
                    "ON CONFLICT (vehicle_id) DO UPDATE SET is_online = $2, last_seen_at = NOW()",
                    {vehicle_id, is_online ? "true" : "false"});

                if (!result.ok()) {
                    LOG(ERROR) << "Failed to update vehicle status: " << result.error();
                }
            }
        });

    LOG(INFO) << "Registered status handler: v2c/*/is_online -> " << FLAGS_kafka_topic_status;

    // Start heartbeat timeout thread
    // Marks vehicles offline if no messages received for heartbeat_timeout_s seconds
    std::thread heartbeat_thread([&db]() {
        LOG(INFO) << "Heartbeat timeout thread started (timeout=" << FLAGS_heartbeat_timeout_s
                  << "s, interval=" << FLAGS_heartbeat_check_interval_s << "s)";

        while (g_running) {
            std::this_thread::sleep_for(std::chrono::seconds(FLAGS_heartbeat_check_interval_s));

            if (!g_running) break;

            if (db && db->is_connected()) {
                std::string timeout_interval = std::to_string(FLAGS_heartbeat_timeout_s) + " seconds";
                auto result = db->execute(
                    "UPDATE vehicles SET is_online = false "
                    "WHERE is_online = true AND last_seen_at < NOW() - INTERVAL '" + timeout_interval + "'");

                if (!result.ok()) {
                    LOG(ERROR) << "Heartbeat timeout check failed: " << result.error();
                } else if (result.affected_rows() > 0) {
                    LOG(INFO) << "Heartbeat timeout: marked " << result.affected_rows()
                              << " vehicles offline";
                }
            }
        }

        LOG(INFO) << "Heartbeat timeout thread stopped";
    });

    // Run the router (blocking)
    router.run();

    // Stop heartbeat thread
    if (heartbeat_thread.joinable()) {
        heartbeat_thread.join();
    }

    // Stop enrichment consumer
    enrichment_consumer.stop();

    // Print final stats
    const auto& stats = router.stats();
    const auto& enrichment_stats = enrichment_consumer.stats();
    LOG(INFO) << "Final stats (v2c - MQTT->Kafka):";
    LOG(INFO) << "  messages_received=" << stats.messages_received;
    LOG(INFO) << "  messages_transformed=" << stats.messages_transformed;
    LOG(INFO) << "  messages_produced=" << stats.messages_produced;
    LOG(INFO) << "  messages_dropped=" << stats.messages_dropped;
    LOG(INFO) << "  transform_errors=" << stats.transform_errors;
    LOG(INFO) << "  produce_errors=" << stats.produce_errors;
    LOG(INFO) << "Final stats (status - is_online):";
    LOG(INFO) << "  status_messages_received=" << stats.status_messages_received;
    LOG(INFO) << "  status_messages_produced=" << stats.status_messages_produced;
    LOG(INFO) << "Final stats (c2v - Kafka->MQTT):";
    LOG(INFO) << "  c2v_messages_received=" << stats.c2v_messages_received;
    LOG(INFO) << "  c2v_messages_transformed=" << stats.c2v_messages_transformed;
    LOG(INFO) << "  c2v_messages_published=" << stats.c2v_messages_published;
    LOG(INFO) << "  c2v_messages_dropped=" << stats.c2v_messages_dropped;
    LOG(INFO) << "  c2v_transform_errors=" << stats.c2v_transform_errors;
    LOG(INFO) << "  c2v_publish_errors=" << stats.c2v_publish_errors;
    LOG(INFO) << "Enrichment stats:";
    LOG(INFO) << "  enrichment_messages=" << enrichment_stats.messages_received;
    LOG(INFO) << "  enrichment_updates=" << enrichment_stats.updates_applied;
    LOG(INFO) << "  enrichment_tombstones=" << enrichment_stats.tombstones_applied;
    LOG(INFO) << "  enrichment_errors=" << enrichment_stats.parse_errors;
    LOG(INFO) << "  vehicles_known=" << context_store->size();

    LOG(INFO) << "Goodbye!";
    return 0;
}
