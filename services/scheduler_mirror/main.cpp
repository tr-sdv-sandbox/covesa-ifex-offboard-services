/// Scheduler Mirror Service for IFEX Offboard
///
/// Consumes scheduler sync messages (content_id=202) from Kafka
/// and persists job state to PostgreSQL.

#include <atomic>
#include <csignal>
#include <memory>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kafka_consumer.hpp"
#include "postgres_client.hpp"
#include "scheduler_offboard_codec.hpp"
#include "scheduler_store.hpp"

// Command-line flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_group, "scheduler-mirror", "Kafka consumer group");
DEFINE_string(kafka_topic, "ifex.scheduler.202", "Kafka topic to consume");

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

int main(int argc, char* argv[]) {
    // Initialize glog and gflags
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    LOG(INFO) << "Scheduler Mirror Service starting...";
    LOG(INFO) << "Kafka: " << FLAGS_kafka_broker << " topic=" << FLAGS_kafka_topic;
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

    // Create scheduler store
    ifex::offboard::SchedulerStore store(db);

    // Create Kafka consumer
    ifex::offboard::KafkaConsumerConfig kafka_config;
    kafka_config.brokers = FLAGS_kafka_broker;
    kafka_config.group_id = FLAGS_kafka_group;
    kafka_config.client_id = "scheduler-mirror";
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
    auto last_commit_time = std::chrono::steady_clock::now();
    const auto commit_interval = std::chrono::seconds(5);

    LOG(INFO) << "Scheduler Mirror running, press Ctrl+C to stop";

    // Message processing loop
    while (!g_shutdown) {
        ifex::offboard::KafkaMessage msg;

        if (!consumer.poll(1000, msg)) {
            continue;
        }

        VLOG(1) << "Received message (" << msg.value.size() << " bytes)";

        // Decode offboard protobuf (contains vehicle_id in metadata)
        auto offboard_msg = ifex::offboard::decode_scheduler_offboard(msg.value);
        if (!offboard_msg) {
            LOG(WARNING) << "Failed to decode scheduler offboard message";
            messages_failed++;
            continue;
        }

        const std::string& vehicle_id = offboard_msg->metadata().vehicle_id();

        // Process offboard message
        try {
            store.process_offboard_message(*offboard_msg);
            messages_processed++;

            VLOG(1) << "Processed " << offboard_msg->sync_message().events_size()
                    << " events from " << vehicle_id;

        } catch (const std::exception& e) {
            LOG(ERROR) << "Error processing sync from " << vehicle_id
                       << ": " << e.what();
            messages_failed++;
        }

        // Periodic commit
        auto now = std::chrono::steady_clock::now();
        if (now - last_commit_time >= commit_interval) {
            consumer.commit();
            last_commit_time = now;

            LOG(INFO) << "Stats: processed=" << messages_processed
                      << " failed=" << messages_failed;
        }
    }

    // Final commit
    consumer.commit();

    LOG(INFO) << "Final stats: processed=" << messages_processed
              << " failed=" << messages_failed;
    LOG(INFO) << "Goodbye!";

    return 0;
}
