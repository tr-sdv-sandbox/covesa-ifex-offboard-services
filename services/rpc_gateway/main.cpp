/// RPC Gateway Service for IFEX Offboard
///
/// Phase 1: Consumes RPC responses (content_id=200) from Kafka
/// and records them in PostgreSQL.
///
/// Phase 2 (TODO): Accepts gRPC calls and forwards them to vehicles
/// via Kafka → MQTT.

#include <atomic>
#include <csignal>
#include <memory>
#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kafka_consumer.hpp"
#include "kafka_producer.hpp"
#include "postgres_client.hpp"
#include "rpc_offboard_codec.hpp"
#include "rpc_store.hpp"

// Command-line flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_group, "rpc-gateway", "Kafka consumer group");
DEFINE_string(kafka_topic_v2c, "ifex.rpc.200", "Kafka topic for v2c (responses)");
DEFINE_string(kafka_topic_c2v, "ifex.c2v.rpc", "Kafka topic for c2v (requests)");

DEFINE_string(postgres_host, "localhost", "PostgreSQL host");
DEFINE_int32(postgres_port, 5432, "PostgreSQL port");
DEFINE_string(postgres_db, "ifex_offboard", "PostgreSQL database");
DEFINE_string(postgres_user, "ifex", "PostgreSQL user");
DEFINE_string(postgres_password, "ifex_dev", "PostgreSQL password");

DEFINE_int32(timeout_check_interval_sec, 30, "Interval to check for timed out requests");

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

void signal_handler(int signum) {
    LOG(INFO) << "Received signal " << signum << ", shutting down...";
    g_shutdown = true;
}

int main(int argc, char* argv[]) {
    // Initialize glog and gflags
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    LOG(INFO) << "RPC Gateway Service starting...";
    LOG(INFO) << "Kafka: " << FLAGS_kafka_broker;
    LOG(INFO) << "  v2c topic (responses): " << FLAGS_kafka_topic_v2c;
    LOG(INFO) << "  c2v topic (requests): " << FLAGS_kafka_topic_c2v;
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

    // Create RPC store
    ifex::offboard::RpcStore store(db);

    // Create Kafka consumer for v2c (responses from vehicles)
    ifex::offboard::KafkaConsumerConfig kafka_config;
    kafka_config.brokers = FLAGS_kafka_broker;
    kafka_config.group_id = FLAGS_kafka_group;
    kafka_config.client_id = "rpc-gateway";
    kafka_config.auto_offset_reset = "earliest";
    kafka_config.enable_auto_commit = false;

    ifex::offboard::KafkaConsumer consumer(kafka_config);

    if (!consumer.subscribe({FLAGS_kafka_topic_v2c})) {
        LOG(FATAL) << "Failed to subscribe to Kafka topic";
        return 1;
    }

    // Create Kafka producer for c2v (requests to vehicles)
    ifex::offboard::KafkaProducerConfig producer_config;
    producer_config.brokers = FLAGS_kafka_broker;
    producer_config.client_id = "rpc-gateway-producer";

    ifex::offboard::KafkaProducer producer(producer_config);

    // Stats
    uint64_t responses_processed = 0;
    uint64_t responses_failed = 0;
    auto last_commit_time = std::chrono::steady_clock::now();
    auto last_timeout_check = std::chrono::steady_clock::now();
    const auto commit_interval = std::chrono::seconds(5);
    const auto timeout_check_interval =
        std::chrono::seconds(FLAGS_timeout_check_interval_sec);

    LOG(INFO) << "RPC Gateway running, press Ctrl+C to stop";

    // Message processing loop
    while (!g_shutdown) {
        // Poll producer for delivery reports
        producer.poll(0);

        // Poll consumer for responses
        ifex::offboard::KafkaMessage msg;

        if (consumer.poll(100, msg)) {
            VLOG(1) << "Received RPC message (" << msg.value.size() << " bytes)";

            // Decode offboard protobuf (contains vehicle_id in metadata)
            auto offboard_msg = ifex::offboard::decode_rpc_offboard(msg.value);

            if (offboard_msg) {
                const std::string& vehicle_id = offboard_msg->metadata().vehicle_id();

                try {
                    store.process_offboard_message(*offboard_msg);
                    responses_processed++;

                    if (offboard_msg->has_response()) {
                        LOG(INFO) << "RPC response from " << vehicle_id
                                  << ": correlation_id="
                                  << offboard_msg->response().correlation_id();
                    }

                } catch (const std::exception& e) {
                    LOG(ERROR) << "Error recording response: " << e.what();
                    responses_failed++;
                }
            } else {
                LOG(WARNING) << "Failed to decode RPC offboard message";
                responses_failed++;
            }
        }

        auto now = std::chrono::steady_clock::now();

        // Periodic commit
        if (now - last_commit_time >= commit_interval) {
            consumer.commit();
            last_commit_time = now;

            LOG(INFO) << "Stats: responses_processed=" << responses_processed
                      << " failed=" << responses_failed;
        }

        // Periodic timeout check
        if (now - last_timeout_check >= timeout_check_interval) {
            int timed_out = store.mark_timed_out_requests(0);
            if (timed_out > 0) {
                LOG(INFO) << "Marked " << timed_out << " requests as timed out";
            }
            last_timeout_check = now;
        }
    }

    // Final commit
    consumer.commit();
    producer.flush(5000);

    LOG(INFO) << "Final stats: responses_processed=" << responses_processed
              << " failed=" << responses_failed;
    LOG(INFO) << "Goodbye!";

    return 0;
}
