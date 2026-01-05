/// Enrichment Exporter Service for IFEX Offboard
///
/// Polls vehicle_enrichment table for changes and exports to Kafka.
/// Uses a compacted topic with vehicle_id as key so consumers always
/// get the latest enrichment data for each vehicle.
///
/// The mqtt_kafka_bridge consumes this topic to enrich messages with
/// fleet_id, region, etc.

#include <atomic>
#include <chrono>
#include <csignal>
#include <memory>
#include <string>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kafka_producer.hpp"
#include "postgres_client.hpp"
#include "vehicle-enrichment.pb.h"

// Command-line flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_topic, "ifex.vehicle.enrichment", "Kafka topic for enrichment data");

DEFINE_string(postgres_host, "localhost", "PostgreSQL host");
DEFINE_int32(postgres_port, 5432, "PostgreSQL port");
DEFINE_string(postgres_db, "ifex_offboard", "PostgreSQL database");
DEFINE_string(postgres_user, "ifex", "PostgreSQL user");
DEFINE_string(postgres_password, "ifex_dev", "PostgreSQL password");

DEFINE_int32(poll_interval_sec, 5, "Interval between polls in seconds");
DEFINE_int32(batch_size, 100, "Max records to fetch per poll");
DEFINE_bool(full_export_on_start, true, "Export all records on startup");

// Global shutdown flag
std::atomic<bool> g_shutdown{false};

void signal_handler(int signum) {
    LOG(INFO) << "Received signal " << signum << ", shutting down...";
    g_shutdown = true;
}

namespace ifex::offboard {

/// Parse PostgreSQL JSONB array string like '["tag1", "tag2"]' into vector
std::vector<std::string> parse_tags_json(const std::string& json) {
    std::vector<std::string> tags;
    // Simple parser for ["tag1", "tag2"] format
    size_t pos = 0;
    while ((pos = json.find('"', pos)) != std::string::npos) {
        size_t start = pos + 1;
        size_t end = json.find('"', start);
        if (end != std::string::npos) {
            tags.push_back(json.substr(start, end - start));
            pos = end + 1;
        } else {
            break;
        }
    }
    return tags;
}

/// Parse PostgreSQL timestamp string to milliseconds since epoch
int64_t parse_timestamp_ms(const std::string& ts) {
    // Simple approximation - for proper parsing use a date library
    // Format: "2026-01-02 04:55:36.123456+00"
    struct tm tm = {};
    if (strptime(ts.c_str(), "%Y-%m-%d %H:%M:%S", &tm) != nullptr) {
        return static_cast<int64_t>(timegm(&tm)) * 1000;
    }
    return 0;
}

/// Enrichment exporter that polls PostgreSQL and publishes to Kafka
class EnrichmentExporter {
public:
    EnrichmentExporter(std::shared_ptr<PostgresClient> db,
                       std::unique_ptr<KafkaProducer> producer,
                       const std::string& topic)
        : db_(std::move(db))
        , producer_(std::move(producer))
        , topic_(topic) {}

    /// Run initial full export of all enrichment data
    uint64_t full_export() {
        LOG(INFO) << "Starting full export of enrichment data...";

        auto result = db_->execute(
            R"(
            SELECT vehicle_id, fleet_id, region, model, year, owner,
                   tags::text as tags_json,
                   updated_at::text,
                   created_at::text
            FROM vehicle_enrichment
            ORDER BY vehicle_id
            )",
            {});

        if (!result.ok()) {
            LOG(ERROR) << "Failed to query enrichment data";
            return 0;
        }

        uint64_t count = 0;
        std::string max_updated_at;

        for (int i = 0; i < result.num_rows(); ++i) {
            auto row = result.row(i);

            // Build protobuf message
            VehicleEnrichment msg;
            msg.set_vehicle_id(row.get_string("vehicle_id"));
            msg.set_fleet_id(row.get_string("fleet_id"));
            msg.set_region(row.get_string("region"));
            msg.set_model(row.get_string("model"));
            msg.set_year(row.get_int("year"));
            msg.set_customer_id(row.get_string("owner"));

            // Parse and add tags
            for (const auto& tag : parse_tags_json(row.get_string("tags_json"))) {
                msg.add_tags(tag);
            }

            // Timestamps
            std::string updated_at = row.get_string("updated_at");
            msg.set_updated_at_ms(parse_timestamp_ms(updated_at));
            msg.set_created_at_ms(parse_timestamp_ms(row.get_string("created_at")));

            // Serialize and publish
            std::string payload;
            msg.SerializeToString(&payload);
            producer_->produce(topic_, msg.vehicle_id(), payload);
            count++;

            // Track max updated_at for cursor
            if (updated_at > max_updated_at) {
                max_updated_at = updated_at;
            }

            // Periodic flush
            if (count % 1000 == 0) {
                producer_->poll(0);
                LOG(INFO) << "Exported " << count << " records...";
            }
        }

        producer_->flush(5000);

        if (!max_updated_at.empty()) {
            cursor_ = max_updated_at;
        }

        LOG(INFO) << "Full export complete: " << count << " records, cursor=" << cursor_;
        return count;
    }

    /// Poll for changes since last cursor
    uint64_t poll_changes() {
        VLOG(1) << "Polling for changes since " << cursor_;

        auto result = db_->execute(
            R"(
            SELECT vehicle_id, fleet_id, region, model, year, owner,
                   tags::text as tags_json,
                   updated_at::text,
                   created_at::text
            FROM vehicle_enrichment
            WHERE updated_at > $1::timestamptz
            ORDER BY updated_at
            LIMIT $2
            )",
            {cursor_, std::to_string(FLAGS_batch_size)});

        if (!result.ok()) {
            LOG(ERROR) << "Failed to poll enrichment changes";
            return 0;
        }

        if (result.num_rows() == 0) {
            VLOG(1) << "No changes found";
            return 0;
        }

        uint64_t count = 0;

        for (int i = 0; i < result.num_rows(); ++i) {
            auto row = result.row(i);

            // Build protobuf message
            VehicleEnrichment msg;
            msg.set_vehicle_id(row.get_string("vehicle_id"));
            msg.set_fleet_id(row.get_string("fleet_id"));
            msg.set_region(row.get_string("region"));
            msg.set_model(row.get_string("model"));
            msg.set_year(row.get_int("year"));
            msg.set_customer_id(row.get_string("owner"));

            for (const auto& tag : parse_tags_json(row.get_string("tags_json"))) {
                msg.add_tags(tag);
            }

            std::string updated_at = row.get_string("updated_at");
            msg.set_updated_at_ms(parse_timestamp_ms(updated_at));
            msg.set_created_at_ms(parse_timestamp_ms(row.get_string("created_at")));

            // Serialize and publish
            std::string payload;
            msg.SerializeToString(&payload);
            producer_->produce(topic_, msg.vehicle_id(), payload);
            count++;

            // Update cursor
            cursor_ = updated_at;
        }

        producer_->flush(1000);

        LOG(INFO) << "Exported " << count << " changed records, new cursor=" << cursor_;
        return count;
    }

    /// Get current cursor position
    const std::string& cursor() const { return cursor_; }

private:
    std::shared_ptr<PostgresClient> db_;
    std::unique_ptr<KafkaProducer> producer_;
    std::string topic_;
    std::string cursor_ = "1970-01-01 00:00:00+00";  // Start from epoch
};

}  // namespace ifex::offboard

int main(int argc, char* argv[]) {
    // Initialize glog and gflags
    google::InitGoogleLogging(argv[0]);
    google::SetStderrLogging(google::INFO);
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Setup signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    LOG(INFO) << "Enrichment Exporter Service starting...";
    LOG(INFO) << "PostgreSQL: " << FLAGS_postgres_db << "@" << FLAGS_postgres_host;
    LOG(INFO) << "Kafka: " << FLAGS_kafka_broker << " topic=" << FLAGS_kafka_topic;
    LOG(INFO) << "Poll interval: " << FLAGS_poll_interval_sec << "s";

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

    // Create Kafka producer
    ifex::offboard::KafkaProducerConfig kafka_config;
    kafka_config.brokers = FLAGS_kafka_broker;
    kafka_config.client_id = "enrichment-exporter";

    auto producer = std::make_unique<ifex::offboard::KafkaProducer>(kafka_config);

    // Create exporter
    ifex::offboard::EnrichmentExporter exporter(db, std::move(producer), FLAGS_kafka_topic);

    // Stats
    uint64_t total_exported = 0;
    uint64_t poll_count = 0;

    // Full export on startup if enabled
    if (FLAGS_full_export_on_start) {
        total_exported = exporter.full_export();
    }

    LOG(INFO) << "Enrichment Exporter running, press Ctrl+C to stop";

    // Polling loop
    auto poll_interval = std::chrono::seconds(FLAGS_poll_interval_sec);
    auto last_poll = std::chrono::steady_clock::now();

    while (!g_shutdown) {
        auto now = std::chrono::steady_clock::now();

        if (now - last_poll >= poll_interval) {
            uint64_t count = exporter.poll_changes();
            total_exported += count;
            poll_count++;
            last_poll = now;

            if (poll_count % 12 == 0) {  // Log every minute at 5s interval
                LOG(INFO) << "Stats: total_exported=" << total_exported
                          << " polls=" << poll_count
                          << " cursor=" << exporter.cursor();
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    LOG(INFO) << "Final stats: total_exported=" << total_exported
              << " polls=" << poll_count;
    LOG(INFO) << "Goodbye!";

    return 0;
}
