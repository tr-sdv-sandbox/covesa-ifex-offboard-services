/// Discovery Mirror Service for IFEX Offboard
///
/// Consumes discovery sync messages (content_id=201) from Kafka
/// and persists service registry state to PostgreSQL.
///
/// Uses hash-based protocol: vehicles send schema hashes, cloud requests
/// full schemas only for unknown hashes.

#include <atomic>
#include <csignal>
#include <memory>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "discovery_store.hpp"
#include "kafka_consumer.hpp"
#include "kafka_producer.hpp"
#include "postgres_client.hpp"

#include "discovery-sync-envelope.pb.h"

// Command-line flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_group, "discovery-mirror", "Kafka consumer group");
DEFINE_string(kafka_topic, "ifex.discovery.201", "Kafka topic to consume (v2c)");
DEFINE_string(kafka_topic_c2v, "ifex.c2v.discovery", "Kafka topic for schema requests (c2v)");

DEFINE_string(postgres_host, "localhost", "PostgreSQL host");
DEFINE_int32(postgres_port, 5432, "PostgreSQL port");
DEFINE_string(postgres_db, "ifex_offboard", "PostgreSQL database");
DEFINE_string(postgres_user, "ifex", "PostgreSQL user");
DEFINE_string(postgres_password, "ifex_dev", "PostgreSQL password");

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

void signal_handler(int signum) {
    LOG(INFO) << "Received signal " << signum << ", shutting down...";
    g_shutdown = true;
}

/// Encode a schema request envelope for c2v
std::string encode_schema_request(
    const std::string& vehicle_id,
    const std::vector<std::string>& hashes) {

    swdv::discovery_sync_envelope::discovery_envelope_t envelope;
    envelope.set_vehicle_id(vehicle_id);

    auto* request = envelope.mutable_request();
    for (const auto& hash : hashes) {
        request->add_hashes(hash);
    }

    return envelope.SerializeAsString();
}

/// Try to decode as discovery_envelope_t (hash-based protocol)
std::optional<swdv::discovery_sync_envelope::discovery_envelope_t>
try_decode_envelope(const std::string& payload) {
    swdv::discovery_sync_envelope::discovery_envelope_t envelope;
    if (envelope.ParseFromString(payload)) {
        // Check if it has one of the expected payloads
        if (envelope.has_manifest() || envelope.has_schemas()) {
            return envelope;
        }
    }
    return std::nullopt;
}

int main(int argc, char* argv[]) {
    // Initialize glog and gflags
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    LOG(INFO) << "Discovery Mirror Service starting...";
    LOG(INFO) << "Kafka: " << FLAGS_kafka_broker;
    LOG(INFO) << "  v2c topic: " << FLAGS_kafka_topic;
    LOG(INFO) << "  c2v topic: " << FLAGS_kafka_topic_c2v;
    LOG(INFO) << "PostgreSQL: " << FLAGS_postgres_db << "@" << FLAGS_postgres_host;

    // Create PostgreSQL client
    ifex::offboard::PostgresConfig pg_config;
    pg_config.host = FLAGS_postgres_host;
    pg_config.port = FLAGS_postgres_port;
    pg_config.database = FLAGS_postgres_db;
    pg_config.user = FLAGS_postgres_user;
    pg_config.password = FLAGS_postgres_password;

    auto db = std::make_shared<ifex::offboard::PostgresClient>(pg_config);

    if (!db->is_connected()) {
        LOG(FATAL) << "Failed to connect to PostgreSQL";
        return 1;
    }

    // Create Kafka producer for c2v schema requests
    ifex::offboard::KafkaProducerConfig producer_config;
    producer_config.brokers = FLAGS_kafka_broker;
    producer_config.client_id = "discovery-mirror-producer";

    auto producer = std::make_shared<ifex::offboard::KafkaProducer>(producer_config);

    // Create discovery store
    ifex::offboard::DiscoveryStore store(db);

    // Wire up schema request callback - produces to c2v Kafka topic
    store.set_schema_request_callback(
        [&producer](const std::string& vehicle_id,
                    const std::vector<std::string>& unknown_hashes) {
            LOG(INFO) << "Requesting " << unknown_hashes.size()
                      << " schemas from vehicle " << vehicle_id;

            std::string payload = encode_schema_request(vehicle_id, unknown_hashes);

            // Kafka key = vehicle_id (for partitioning)
            if (!producer->produce(FLAGS_kafka_topic_c2v, vehicle_id, payload)) {
                LOG(ERROR) << "Failed to produce schema request to "
                           << FLAGS_kafka_topic_c2v;
            } else {
                LOG(INFO) << "Sent schema request to " << vehicle_id
                          << " (" << payload.size() << " bytes)";
            }
        });

    // Create Kafka consumer
    ifex::offboard::KafkaConsumerConfig kafka_config;
    kafka_config.brokers = FLAGS_kafka_broker;
    kafka_config.group_id = FLAGS_kafka_group;
    kafka_config.client_id = "discovery-mirror";
    kafka_config.auto_offset_reset = "earliest";
    kafka_config.enable_auto_commit = false;

    ifex::offboard::KafkaConsumer consumer(kafka_config);

    if (!consumer.subscribe({FLAGS_kafka_topic})) {
        LOG(FATAL) << "Failed to subscribe to Kafka topic";
        return 1;
    }

    // Stats
    uint64_t messages_processed = 0;
    uint64_t messages_failed = 0;
    uint64_t manifests_received = 0;
    uint64_t schemas_received = 0;
    auto last_commit_time = std::chrono::steady_clock::now();
    const auto commit_interval = std::chrono::seconds(5);

    LOG(INFO) << "Discovery Mirror running, press Ctrl+C to stop";

    // Message processing loop
    while (!g_shutdown) {
        ifex::offboard::KafkaMessage msg;

        if (!consumer.poll(1000, msg)) {
            continue;
        }

        VLOG(1) << "Received message (" << msg.value.size() << " bytes)";

        try {
            // Decode as hash-based envelope
            auto envelope = try_decode_envelope(msg.value);

            if (envelope) {
                const std::string& vehicle_id = envelope->vehicle_id();

                if (envelope->has_manifest()) {
                    // Vehicle sent hash manifest - process and request unknown
                    store.process_hash_manifest(vehicle_id, envelope->manifest());
                    manifests_received++;
                    messages_processed++;

                    LOG(INFO) << "Processed hash manifest from " << vehicle_id
                              << " with " << envelope->manifest().hashes_size() << " hashes";

                } else if (envelope->has_schemas()) {
                    // Vehicle sent schema response - store the schemas
                    store.process_schema_response(vehicle_id, envelope->schemas());
                    schemas_received++;
                    messages_processed++;

                    LOG(INFO) << "Processed schema response from " << vehicle_id
                              << " with " << envelope->schemas().schemas_size() << " schemas";
                }

            } else {
                LOG(WARNING) << "Failed to decode discovery envelope";
                messages_failed++;
            }

        } catch (const std::exception& e) {
            LOG(ERROR) << "Error processing discovery message: " << e.what();
            messages_failed++;
        }

        // Periodic commit and stats
        auto now = std::chrono::steady_clock::now();
        if (now - last_commit_time >= commit_interval) {
            consumer.commit();
            producer->flush();
            last_commit_time = now;

            LOG(INFO) << "Stats: processed=" << messages_processed
                      << " (manifests=" << manifests_received
                      << ", schemas=" << schemas_received
                      << ") failed=" << messages_failed;
        }
    }

    // Final commit and flush
    consumer.commit();
    producer->flush();

    LOG(INFO) << "Final stats: processed=" << messages_processed
              << " (manifests=" << manifests_received
              << ", schemas=" << schemas_received
              << ") failed=" << messages_failed;
    LOG(INFO) << "Goodbye!";

    return 0;
}
