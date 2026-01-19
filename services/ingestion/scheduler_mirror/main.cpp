/// Scheduler Mirror Service for IFEX Offboard
///
/// Bidirectional state synchronization between cloud and vehicle schedulers.
///
/// - Receives V2C_SyncMessage from vehicles (via Kafka)
/// - Sends C2V_SyncMessage to vehicles with cloud-side job state
/// - Handles TriggerJobRequest for immediate job execution
///
/// This is PURE STATE SYNC - no imperative commands except TriggerJob.

#include <atomic>
#include <csignal>
#include <memory>
#include <string>
#include <random>
#include <sstream>
#include <iomanip>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "kafka_consumer.hpp"
#include "kafka_producer.hpp"
#include "postgres_client.hpp"
#include "scheduler_store.hpp"
#include "scheduler-sync-v2.pb.h"

// Command-line flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_group, "scheduler-mirror", "Kafka consumer group");
DEFINE_string(kafka_topic, "ifex.scheduler.202", "Kafka topic for v2c sync messages");
DEFINE_string(kafka_c2v_topic, "ifex.c2v.scheduler", "Kafka topic for c2v sync messages");
DEFINE_string(kafka_status_topic, "ifex.status", "Kafka topic for vehicle status");

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

// Send C2V_SyncMessage with cloud's pending jobs to a vehicle
// Implements quiescence detection per Scheduler Sync Protocol v2.4 Section 5.6
void send_c2v_sync(
    const std::string& vehicle_id,
    ifex::offboard::SchedulerStore& store,
    std::shared_ptr<ifex::offboard::KafkaProducer> producer,
    const std::string& c2v_topic,
    bool force = false) {

    // Check quiescence state
    auto quiescence = store.get_quiescence_state(vehicle_id);

    if (!force && quiescence.is_quiescent) {
        VLOG(1) << "Vehicle " << vehicle_id << " is QUIESCENT (no pending jobs), no C2V needed";
        return;
    }

    // Get pending cloud jobs for this vehicle
    auto pending_jobs = store.get_pending_cloud_jobs(vehicle_id);

    if (pending_jobs.empty() && !force) {
        // No pending jobs means we're quiescent - skip sending
        // Note: We don't compare checksums - vehicle's checksum is for tracking, not equality
        VLOG(1) << "No pending cloud jobs for " << vehicle_id << ", skipping C2V";
        return;
    }

    LOG(INFO) << "Vehicle " << vehicle_id << " sync: " << pending_jobs.size()
              << " pending jobs, cloud_checksum=" << quiescence.cloud_checksum
              << ", last_v2c_checksum=" << quiescence.last_v2c_checksum;

    // Build C2V_SyncMessage
    swdv::scheduler_sync_v2::C2V_SyncMessage c2v_msg;
    c2v_msg.set_vehicle_id(vehicle_id);
    c2v_msg.set_sync_timestamp_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());

    // Add all pending jobs
    for (const auto& job : pending_jobs) {
        *c2v_msg.add_jobs() = job;
    }

    // Set checksums for quiescence detection (per spec section 5.1)
    c2v_msg.set_state_checksum(quiescence.cloud_checksum);
    c2v_msg.set_last_seen_v2c_checksum(quiescence.last_v2c_checksum);

    // Serialize and send
    std::string serialized;
    if (!c2v_msg.SerializeToString(&serialized)) {
        LOG(ERROR) << "Failed to serialize C2V_SyncMessage for " << vehicle_id;
        return;
    }

    if (producer->produce(c2v_topic, vehicle_id, serialized)) {
        LOG(INFO) << "Sent C2V sync to " << vehicle_id << ": " << pending_jobs.size()
                  << " jobs, checksum=" << quiescence.cloud_checksum;
    } else {
        LOG(ERROR) << "Failed to send C2V sync to " << vehicle_id;
    }
}

// Handle TriggerJobRequest - send TriggerJobRequest to vehicle
void handle_trigger_request(
    const std::string& vehicle_id,
    const std::string& job_id,
    const std::string& requester_id,
    std::shared_ptr<ifex::offboard::KafkaProducer> producer,
    const std::string& c2v_topic) {

    swdv::scheduler_sync_v2::TriggerJobRequest trigger;
    trigger.set_job_id(job_id);
    trigger.set_requester_id(requester_id);
    trigger.set_timestamp_ms(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    // Default: no expiry (0)

    std::string serialized;
    if (!trigger.SerializeToString(&serialized)) {
        LOG(ERROR) << "Failed to serialize TriggerJobRequest for " << job_id;
        return;
    }

    if (producer->produce(c2v_topic, vehicle_id, serialized)) {
        LOG(INFO) << "Sent TriggerJobRequest for job " << job_id
                  << " to vehicle " << vehicle_id;
    } else {
        LOG(ERROR) << "Failed to send TriggerJobRequest for " << job_id;
    }
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

    // Create Kafka producer for c2v sync messages
    ifex::offboard::KafkaProducerConfig producer_config;
    producer_config.brokers = FLAGS_kafka_broker;
    producer_config.client_id = "scheduler-mirror-producer";

    auto producer = std::make_shared<ifex::offboard::KafkaProducer>(producer_config);

    LOG(INFO) << "C2V Kafka producer initialized, topic=" << FLAGS_kafka_c2v_topic;

    // Create Kafka consumer
    ifex::offboard::KafkaConsumerConfig kafka_config;
    kafka_config.brokers = FLAGS_kafka_broker;
    kafka_config.group_id = FLAGS_kafka_group;
    kafka_config.client_id = "scheduler-mirror";
    kafka_config.auto_offset_reset = "earliest";
    kafka_config.enable_auto_commit = false;

    ifex::offboard::KafkaConsumer consumer(kafka_config);

    // Subscribe to scheduler sync and status topics
    if (!consumer.subscribe({FLAGS_kafka_topic, FLAGS_kafka_status_topic})) {
        LOG(FATAL) << "Failed to subscribe to Kafka topics";
        return 1;
    }

    LOG(INFO) << "Subscribed to topics: " << FLAGS_kafka_topic << ", " << FLAGS_kafka_status_topic;

    // Stats
    uint64_t messages_processed = 0;
    uint64_t messages_failed = 0;
    uint64_t syncs_sent = 0;
    auto last_commit_time = std::chrono::steady_clock::now();
    const auto commit_interval = std::chrono::seconds(5);

    LOG(INFO) << "Scheduler Mirror running (pure state sync mode)";

    // Message processing loop
    while (!g_shutdown) {
        ifex::offboard::KafkaMessage msg;

        if (!consumer.poll(1000, msg)) {
            continue;
        }

        VLOG(1) << "Received message from topic=" << msg.topic
                << " key=" << msg.key << " (" << msg.value.size() << " bytes)";

        // Handle status messages (vehicle online/offline)
        if (msg.topic == FLAGS_kafka_status_topic) {
            std::string vehicle_id = msg.key;
            bool is_online = (msg.value == "1");

            if (vehicle_id.empty()) {
                LOG(WARNING) << "Status message with empty vehicle_id";
                continue;
            }

            if (is_online) {
                LOG(INFO) << "Vehicle " << vehicle_id << " came ONLINE - forcing C2V sync";
                // Force sync on reconnect to ensure vehicle has latest state
                send_c2v_sync(vehicle_id, store, producer, FLAGS_kafka_c2v_topic, true /* force */);
                syncs_sent++;
            } else {
                VLOG(1) << "Vehicle " << vehicle_id << " went offline";
            }
            continue;
        }

        // Handle scheduler sync messages (content_id=202)
        // Try parsing as V2C_SyncMessage (vehicle → cloud)
        swdv::scheduler_sync_v2::V2C_SyncMessage v2c_msg;
        if (v2c_msg.ParseFromString(msg.value) && !v2c_msg.vehicle_id().empty()) {
            std::string vehicle_id = v2c_msg.vehicle_id();
            std::string fleet_id;
            std::string region;

            // Extract enrichment from Kafka key if present
            if (!msg.key.empty()) {
                size_t pos1 = msg.key.find(':');
                if (pos1 != std::string::npos) {
                    size_t pos2 = msg.key.find(':', pos1 + 1);
                    if (pos2 != std::string::npos) {
                        fleet_id = msg.key.substr(pos1 + 1, pos2 - pos1 - 1);
                        region = msg.key.substr(pos2 + 1);
                    }
                }
            }

            try {
                // Per Scheduler Sync Protocol v2.4: If the incoming V2C checksum
                // matches the last stored checksum, the vehicle state hasn't changed.
                // Drop the message to avoid overwriting pending cloud changes.
                if (v2c_msg.state_checksum() > 0) {
                    uint64_t last_checksum = store.get_last_v2c_checksum(vehicle_id);
                    if (last_checksum == v2c_msg.state_checksum()) {
                        VLOG(1) << "V2C from " << vehicle_id << ": checksum unchanged ("
                                << v2c_msg.state_checksum() << "), dropping duplicate";
                        messages_processed++;
                        continue;  // Skip processing - vehicle state unchanged
                    }
                }

                store.process_v2_sync_message(vehicle_id, fleet_id, region, v2c_msg);
                messages_processed++;

                // Store the V2C checksum for quiescence detection
                // Per spec section 5.6: track vehicle's last reported checksum
                if (v2c_msg.state_checksum() > 0) {
                    store.store_v2c_checksum(vehicle_id, v2c_msg.state_checksum());
                }

                LOG(INFO) << "V2C Sync from " << vehicle_id << ": "
                          << v2c_msg.jobs_size() << " jobs, "
                          << v2c_msg.executions_size() << " executions"
                          << ", v2c_checksum=" << v2c_msg.state_checksum();

                // After receiving vehicle state, check if we need to send cloud state back
                // Quiescence detection: only send if not quiescent
                send_c2v_sync(vehicle_id, store, producer, FLAGS_kafka_c2v_topic);
                syncs_sent++;

            } catch (const std::exception& e) {
                LOG(ERROR) << "Error processing V2C sync from " << vehicle_id
                           << ": " << e.what();
                messages_failed++;
            }
            continue;
        }

        // Try parsing as TriggerJobResponse (vehicle → cloud)
        swdv::scheduler_sync_v2::TriggerJobResponse trigger_resp;
        if (trigger_resp.ParseFromString(msg.value) && !trigger_resp.job_id().empty()) {
            LOG(INFO) << "TriggerJobResponse: job=" << trigger_resp.job_id()
                      << " accepted=" << trigger_resp.accepted()
                      << (trigger_resp.accepted() ? "" : " error=" + trigger_resp.error_message());
            messages_processed++;
            continue;
        }

        // Unknown message type
        LOG(WARNING) << "Unknown message type on scheduler topic ("
                     << msg.value.size() << " bytes)";
        messages_failed++;

        // Periodic commit
        auto now = std::chrono::steady_clock::now();
        if (now - last_commit_time >= commit_interval) {
            consumer.commit();
            last_commit_time = now;

            LOG(INFO) << "Stats: processed=" << messages_processed
                      << " syncs_sent=" << syncs_sent
                      << " failed=" << messages_failed;
        }
    }

    // Final commit
    consumer.commit();

    LOG(INFO) << "Final stats: processed=" << messages_processed
              << " syncs_sent=" << syncs_sent
              << " failed=" << messages_failed;
    LOG(INFO) << "Goodbye!";

    return 0;
}
