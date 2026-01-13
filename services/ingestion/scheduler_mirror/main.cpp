/// Scheduler Mirror Service for IFEX Offboard
///
/// Consumes scheduler sync messages (content_id=202) from Kafka
/// and persists job state to PostgreSQL.
///
/// Also reconciles offboard_calendar with vehicle state, sending
/// commands to align vehicle with cloud state.

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
#include "scheduler-command-envelope.pb.h"
#include "scheduler-sync-v2.pb.h"

// Command-line flags
DEFINE_string(kafka_broker, "localhost:9092", "Kafka broker address");
DEFINE_string(kafka_group, "scheduler-mirror", "Kafka consumer group");
DEFINE_string(kafka_topic, "ifex.scheduler.202", "Kafka topic to consume");
DEFINE_string(kafka_c2v_topic, "ifex.c2v.scheduler", "Kafka topic for c2v commands");

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

// Generate unique command ID
std::string generate_command_id() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dist;

    std::stringstream ss;
    ss << "cmd-" << std::hex << std::setfill('0') << std::setw(16) << dist(gen);
    return ss.str();
}

// Build scheduler command protobuf from ReconcileCommand
swdv::scheduler_command_envelope::scheduler_command_t build_scheduler_command(
    const ifex::offboard::ReconcileCommand& cmd) {

    swdv::scheduler_command_envelope::scheduler_command_t proto;
    proto.set_command_id(generate_command_id());
    proto.set_timestamp_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    proto.set_requester_id("scheduler-mirror-reconcile");

    switch (cmd.type) {
        case ifex::offboard::ReconcileCommand::CREATE: {
            proto.set_type(swdv::scheduler_command_envelope::COMMAND_CREATE_JOB);
            auto* job_def = proto.mutable_create_job();
            job_def->set_job_id(cmd.job_id);
            job_def->set_title(cmd.title);
            job_def->set_service(cmd.service);
            job_def->set_method(cmd.method);
            job_def->set_parameters_json(cmd.parameters_json);
            job_def->set_scheduled_time_ms(cmd.scheduled_time_ms);
            job_def->set_recurrence_rule(cmd.recurrence_rule);
            job_def->set_end_time_ms(cmd.end_time_ms);
            job_def->set_wake_policy(
                static_cast<swdv::scheduler_command_envelope::wake_policy_t>(cmd.wake_policy));
            job_def->set_sleep_policy(
                static_cast<swdv::scheduler_command_envelope::sleep_policy_t>(cmd.sleep_policy));
            job_def->set_wake_lead_time_s(cmd.wake_lead_time_s);
            break;
        }
        case ifex::offboard::ReconcileCommand::UPDATE: {
            proto.set_type(swdv::scheduler_command_envelope::COMMAND_UPDATE_JOB);
            auto* job_upd = proto.mutable_update_job();
            job_upd->set_job_id(cmd.job_id);
            job_upd->set_title(cmd.title);
            job_upd->set_scheduled_time_ms(cmd.scheduled_time_ms);
            job_upd->set_recurrence_rule(cmd.recurrence_rule);
            job_upd->set_parameters_json(cmd.parameters_json);
            job_upd->set_end_time_ms(cmd.end_time_ms);
            job_upd->set_wake_policy(
                static_cast<swdv::scheduler_command_envelope::wake_policy_t>(cmd.wake_policy));
            job_upd->set_sleep_policy(
                static_cast<swdv::scheduler_command_envelope::sleep_policy_t>(cmd.sleep_policy));
            job_upd->set_wake_lead_time_s(cmd.wake_lead_time_s);
            break;
        }
        case ifex::offboard::ReconcileCommand::DELETE: {
            proto.set_type(swdv::scheduler_command_envelope::COMMAND_DELETE_JOB);
            proto.set_delete_job_id(cmd.job_id);
            break;
        }
    }

    return proto;
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

    // Create Kafka producer for c2v commands
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

        // Parse V2 protocol message
        swdv::scheduler_sync_v2::V2C_SyncMessage v2_msg;
        if (!v2_msg.ParseFromString(msg.value) || v2_msg.vehicle_id().empty()) {
            LOG(WARNING) << "Failed to parse V2 sync message";
            messages_failed++;
            continue;
        }

        std::string vehicle_id = v2_msg.vehicle_id();
        std::string fleet_id;
        std::string region;

        // Extract enrichment from Kafka key if present
        // Key format: "vehicle_id" or "vehicle_id:fleet_id:region"
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
            store.process_v2_sync_message(vehicle_id, fleet_id, region, v2_msg);
            messages_processed++;

            LOG(INFO) << "Sync: " << vehicle_id << " - " << v2_msg.jobs_size()
                      << " jobs, " << v2_msg.executions_size() << " executions";

        } catch (const std::exception& e) {
            LOG(ERROR) << "Error processing sync from " << vehicle_id
                       << ": " << e.what();
            messages_failed++;
            continue;
        }

        // After processing vehicle sync, reconcile with offboard calendar
        // This pushes any pending cloud-side jobs to the vehicle
        if (store.has_pending_offboard_items(vehicle_id)) {
            LOG(INFO) << "Vehicle " << vehicle_id << " has pending offboard items, reconciling...";

            auto commands = store.reconcile_with_offboard(vehicle_id);

            for (const auto& cmd : commands) {
                auto proto = build_scheduler_command(cmd);
                std::string serialized;
                if (!proto.SerializeToString(&serialized)) {
                    LOG(ERROR) << "Failed to serialize reconcile command for " << cmd.job_id;
                    continue;
                }

                // Send to c2v topic with vehicle_id as key
                if (producer->produce(FLAGS_kafka_c2v_topic, vehicle_id, serialized)) {
                    LOG(INFO) << "Sent reconcile command " << cmd.job_id
                              << " type=" << static_cast<int>(cmd.type)
                              << " to vehicle " << vehicle_id;
                } else {
                    LOG(ERROR) << "Failed to send reconcile command " << cmd.job_id;
                }
            }

            if (!commands.empty()) {
                LOG(INFO) << "Reconciliation complete: sent " << commands.size()
                          << " commands to " << vehicle_id;
            }
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
