/**
 * @file e2e_test_fixture.cpp
 * @brief Implementation of shared E2E test infrastructure
 */

#include "e2e_test_fixture.hpp"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <linux/limits.h>
#include <signal.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace ifex::offboard::test {

// Static member definitions
pid_t E2ETestInfrastructure::mqtt_kafka_bridge_pid_ = 0;
pid_t E2ETestInfrastructure::discovery_mirror_pid_ = 0;
pid_t E2ETestInfrastructure::scheduler_mirror_pid_ = 0;
std::unique_ptr<PostgresClient> E2ETestInfrastructure::db_ = nullptr;
bool E2ETestInfrastructure::is_running_ = false;

// Cleanup handler for atexit
static void cleanup_on_exit() {
    if (E2ETestInfrastructure::IsRunning()) {
        LOG(INFO) << "atexit: Cleaning up E2E infrastructure...";
        E2ETestInfrastructure::StopInfrastructure();
    }
}

// PID file for tracking processes we started
static const char* PID_FILE = "/tmp/e2e_test_pids";

// Save PIDs to file so we can clean up on next run
static void save_pids(pid_t bridge, pid_t discovery, pid_t scheduler) {
    std::ofstream f(PID_FILE);
    if (f) {
        f << bridge << "\n" << discovery << "\n" << scheduler << "\n";
    }
}

// Kill any stale E2E test processes from previous runs
static void kill_stale_processes() {
    // Read PIDs from file and kill them
    std::ifstream f(PID_FILE);
    if (f) {
        pid_t pid;
        while (f >> pid) {
            if (pid > 0) {
                kill(pid, SIGKILL);
            }
        }
    }
    // Remove the PID file
    std::remove(PID_FILE);

    // Give processes time to terminate
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

std::string E2ETestInfrastructure::GetExecutableDir() {
    char buf[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    if (len == -1) {
        return ".";
    }
    buf[len] = '\0';
    std::string path(buf);
    size_t pos = path.rfind('/');
    return (pos != std::string::npos) ? path.substr(0, pos) : ".";
}

std::string E2ETestInfrastructure::GetBinaryPath(const std::string& name) {
    // Test binaries are in tests/, service binaries are one level up
    return GetExecutableDir() + "/../" + name;
}

bool E2ETestInfrastructure::VerifyDockerInfrastructure() {
    LOG(INFO) << "Verifying Docker infrastructure...";

    // Check PostgreSQL
    PostgresConfig pg_config;
    pg_config.host = POSTGRES_HOST;
    pg_config.port = POSTGRES_PORT;
    pg_config.database = POSTGRES_DB;
    pg_config.user = POSTGRES_USER;
    pg_config.password = POSTGRES_PASSWORD;

    db_ = std::make_unique<PostgresClient>(pg_config);
    if (!db_->is_connected()) {
        LOG(ERROR) << "PostgreSQL not available at " << POSTGRES_HOST << ":" << POSTGRES_PORT;
        LOG(ERROR) << "Run: ./deploy/start-infra.sh";
        return false;
    }
    LOG(INFO) << "  PostgreSQL: OK";

    // Check MQTT (try to connect briefly)
    // We'll verify this when starting the bridge

    // Check Kafka (we'll verify when bridge starts)

    return true;
}

bool E2ETestInfrastructure::VerifyVehicleImageExists() {
    LOG(INFO) << "Checking for vehicle Docker image...";

    int ret = system("docker image inspect ifex-vehicle:latest >/dev/null 2>&1");
    if (ret != 0) {
        LOG(ERROR) << "Docker image 'ifex-vehicle:latest' not found!";
        LOG(ERROR) << "";
        LOG(ERROR) << "Build it with:";
        LOG(ERROR) << "  cd ../covesa-ifex-core && ./build-test-container.sh";
        LOG(ERROR) << "";
        return false;
    }
    LOG(INFO) << "  ifex-vehicle:latest: OK";
    return true;
}

bool E2ETestInfrastructure::CreateKafkaTopics() {
    LOG(INFO) << "Creating Kafka topics...";

    // Topics needed for bidirectional communication
    const std::vector<std::string> topics = {
        "ifex.discovery.201",      // v2c: vehicle discovery sync
        "ifex.scheduler.202",      // v2c: vehicle scheduler sync
        "ifex.rpc.200",            // v2c: RPC responses
        "ifex.status",             // v2c: vehicle status
        "ifex.c2v.discovery",      // c2v: schema requests to vehicle
        "ifex.c2v.scheduler",      // c2v: scheduler commands to vehicle
        "ifex.c2v.rpc"             // c2v: RPC requests to vehicle
    };

    for (const auto& topic : topics) {
        std::string cmd = "docker exec ifex-kafka /opt/kafka/bin/kafka-topics.sh "
                          "--bootstrap-server localhost:9092 "
                          "--create --if-not-exists "
                          "--topic " + topic + " "
                          "--partitions 1 "
                          "--replication-factor 1 "
                          ">/dev/null 2>&1";
        system(cmd.c_str());
    }

    LOG(INFO) << "  Created " << topics.size() << " Kafka topics";
    return true;
}

bool E2ETestInfrastructure::StartBridgeServices() {
    LOG(INFO) << "Starting bridge services...";

    std::string bin_dir = GetBinaryPath("");

    // Start mqtt_kafka_bridge
    mqtt_kafka_bridge_pid_ = fork();
    if (mqtt_kafka_bridge_pid_ == 0) {
        std::string binary = bin_dir + "mqtt_kafka_bridge";
        execl(binary.c_str(), "mqtt_kafka_bridge",
              "--mqtt_host", MQTT_HOST,
              "--kafka_broker", KAFKA_BROKER,
              "--postgres_host", POSTGRES_HOST,
              "--logtostderr",
              nullptr);
        LOG(ERROR) << "Failed to exec mqtt_kafka_bridge";
        _exit(1);
    }
    if (mqtt_kafka_bridge_pid_ < 0) {
        LOG(ERROR) << "Failed to fork mqtt_kafka_bridge";
        return false;
    }
    LOG(INFO) << "  mqtt_kafka_bridge: PID " << mqtt_kafka_bridge_pid_;

    // Start discovery_mirror
    discovery_mirror_pid_ = fork();
    if (discovery_mirror_pid_ == 0) {
        std::string binary = bin_dir + "discovery_mirror";
        execl(binary.c_str(), "discovery_mirror",
              "--kafka_broker", KAFKA_BROKER,
              "--postgres_host", POSTGRES_HOST,
              "--logtostderr",
              nullptr);
        LOG(ERROR) << "Failed to exec discovery_mirror";
        _exit(1);
    }
    if (discovery_mirror_pid_ < 0) {
        LOG(ERROR) << "Failed to fork discovery_mirror";
        return false;
    }
    LOG(INFO) << "  discovery_mirror: PID " << discovery_mirror_pid_;

    // Start scheduler_mirror
    scheduler_mirror_pid_ = fork();
    if (scheduler_mirror_pid_ == 0) {
        std::string binary = bin_dir + "scheduler_mirror";
        execl(binary.c_str(), "scheduler_mirror",
              "--kafka_broker", KAFKA_BROKER,
              "--postgres_host", POSTGRES_HOST,
              "--logtostderr",
              nullptr);
        LOG(ERROR) << "Failed to exec scheduler_mirror";
        _exit(1);
    }
    if (scheduler_mirror_pid_ < 0) {
        LOG(ERROR) << "Failed to fork scheduler_mirror";
        return false;
    }
    LOG(INFO) << "  scheduler_mirror: PID " << scheduler_mirror_pid_;

    // Save PIDs to file for cleanup on next run (in case of crash)
    save_pids(mqtt_kafka_bridge_pid_, discovery_mirror_pid_, scheduler_mirror_pid_);

    // Give services time to start
    std::this_thread::sleep_for(std::chrono::seconds(2));

    return true;
}

void E2ETestInfrastructure::StopBridgeServices() {
    LOG(INFO) << "Stopping bridge services...";

    auto stop_process = [](pid_t& pid, const char* name) {
        if (pid > 0) {
            LOG(INFO) << "  Stopping " << name << " (PID " << pid << ")";
            kill(pid, SIGTERM);
            int status;
            waitpid(pid, &status, 0);
            pid = 0;
        }
    };

    stop_process(mqtt_kafka_bridge_pid_, "mqtt_kafka_bridge");
    stop_process(discovery_mirror_pid_, "discovery_mirror");
    stop_process(scheduler_mirror_pid_, "scheduler_mirror");

    // Remove PID file after clean shutdown
    std::remove(PID_FILE);
}

bool E2ETestInfrastructure::StartVehicleContainer() {
    LOG(INFO) << "Starting vehicle container...";

    // Stop any existing container with same name
    system(("docker rm -f " + std::string(VEHICLE_CONTAINER_NAME) + " >/dev/null 2>&1").c_str());

    // Create the simulation network if it doesn't exist
    system("docker network create ifex-simulation 2>/dev/null || true");

    // Connect mosquitto to the simulation network so vehicle can reach it by name
    system("docker network connect ifex-simulation ifex-mosquitto 2>/dev/null || true");

    // Start vehicle container on bridge network (not --network host)
    // This allows multiple vehicles to run simultaneously, each with their own ports
    std::string cmd = "docker run -d"
                      " --name " + std::string(VEHICLE_CONTAINER_NAME) +
                      " --network ifex-simulation"
                      " -e VEHICLE_ID=" + std::string(VEHICLE_ID) +
                      " -e MQTT_HOST=ifex-mosquitto"
                      " -e MQTT_PORT=" + std::to_string(MQTT_PORT) +
                      " -e START_TEST_SERVICES=true"
                      " " + std::string(VEHICLE_IMAGE);

    LOG(INFO) << "  Command: " << cmd;

    int ret = system(cmd.c_str());
    if (ret != 0) {
        LOG(ERROR) << "Failed to start vehicle container";
        return false;
    }

    LOG(INFO) << "  Vehicle container started: " << VEHICLE_CONTAINER_NAME;
    return true;
}

void E2ETestInfrastructure::StopVehicleContainer() {
    LOG(INFO) << "Stopping vehicle container...";

    std::string cmd = "docker rm -f " + std::string(VEHICLE_CONTAINER_NAME) + " >/dev/null 2>&1";
    system(cmd.c_str());

    LOG(INFO) << "  Vehicle container stopped";
}

bool E2ETestInfrastructure::StartInfrastructure(bool need_vehicle) {
    LOG(INFO) << "========================================";
    LOG(INFO) << "Starting E2E Test Infrastructure";
    LOG(INFO) << "  need_vehicle: " << (need_vehicle ? "yes" : "no");
    LOG(INFO) << "========================================";

    // Register cleanup handler for process exit (only once)
    static bool atexit_registered = false;
    if (!atexit_registered) {
        std::atexit(cleanup_on_exit);
        atexit_registered = true;
        LOG(INFO) << "Registered atexit cleanup handler";
    }

    // Kill any stale processes from previous test runs
    LOG(INFO) << "Killing any stale processes from previous runs...";
    kill_stale_processes();

    // Verify prerequisites
    if (!VerifyDockerInfrastructure()) {
        return false;
    }

    if (need_vehicle && !VerifyVehicleImageExists()) {
        return false;
    }

    // Recreate database schema for clean test state
    LOG(INFO) << "Recreating database schema...";
    ResetDatabase();

    // Create Kafka topics (idempotent, ensures c2v topics exist)
    if (!CreateKafkaTopics()) {
        LOG(ERROR) << "Failed to create Kafka topics";
        return false;
    }

    // Also stop any leftover vehicle container from previous runs
    if (need_vehicle) {
        StopVehicleContainer();
    }

    // Create test vehicle in database (for vehicle tests)
    if (need_vehicle) {
        auto result = db_->execute(R"(
            INSERT INTO vehicles (vehicle_id, is_online, first_seen_at, last_seen_at)
            VALUES ($1, false, NOW(), NOW())
            ON CONFLICT (vehicle_id) DO UPDATE SET
                is_online = false,
                last_seen_at = NOW()
        )", {VEHICLE_ID});

        if (!result.ok()) {
            LOG(ERROR) << "Failed to create test vehicle: " << result.error();
            return false;
        }

        // Also add to vehicle_enrichment (required for discovery queries)
        result = db_->execute(R"(
            INSERT INTO vehicle_enrichment (vehicle_id, fleet_id, region)
            VALUES ($1, 'e2e-test-fleet', 'e2e-test-region')
            ON CONFLICT (vehicle_id) DO NOTHING
        )", {VEHICLE_ID});

        if (!result.ok()) {
            LOG(ERROR) << "Failed to create vehicle enrichment: " << result.error();
            return false;
        }
    }

    // Start bridge services
    if (!StartBridgeServices()) {
        return false;
    }

    // Start vehicle container (only if needed)
    if (need_vehicle) {
        if (!StartVehicleContainer()) {
            StopBridgeServices();
            return false;
        }
    }

    is_running_ = true;

    LOG(INFO) << "========================================";
    LOG(INFO) << "E2E Infrastructure Ready";
    if (need_vehicle) {
        LOG(INFO) << "  Vehicle ID: " << VEHICLE_ID;
    } else {
        LOG(INFO) << "  (bridge services only, no vehicle)";
    }
    LOG(INFO) << "========================================";

    return true;
}

void E2ETestInfrastructure::StopInfrastructure() {
    if (!is_running_) {
        return;
    }

    LOG(INFO) << "========================================";
    LOG(INFO) << "Stopping E2E Test Infrastructure";
    LOG(INFO) << "========================================";

    // Stop vehicle container
    StopVehicleContainer();

    // Stop bridge services
    StopBridgeServices();

    // Clean up test data
    if (db_ && db_->is_connected()) {
        LOG(INFO) << "Cleaning up test data...";

        // Delete in correct order for FK constraints
        db_->execute("DELETE FROM job_executions WHERE vehicle_id = $1", {VEHICLE_ID});
        db_->execute("DELETE FROM jobs WHERE vehicle_id = $1", {VEHICLE_ID});
        db_->execute("DELETE FROM vehicle_schemas WHERE vehicle_id = $1", {VEHICLE_ID});
        db_->execute("DELETE FROM sync_state WHERE vehicle_id = $1", {VEHICLE_ID});
        db_->execute("DELETE FROM vehicle_enrichment WHERE vehicle_id = $1", {VEHICLE_ID});
        db_->execute("DELETE FROM vehicles WHERE vehicle_id = $1", {VEHICLE_ID});

        LOG(INFO) << "  Test data cleaned up";
    }

    db_.reset();
    is_running_ = false;

    LOG(INFO) << "========================================";
    LOG(INFO) << "E2E Infrastructure Stopped";
    LOG(INFO) << "========================================";
}

bool E2ETestInfrastructure::WaitForVehicleOnline(const std::string& vehicle_id,
                                                  std::chrono::seconds timeout) {
    LOG(INFO) << "Waiting for vehicle " << vehicle_id << " to come online...";

    auto deadline = std::chrono::steady_clock::now() + timeout;

    while (std::chrono::steady_clock::now() < deadline) {
        if (!db_ || !db_->is_connected()) {
            LOG(ERROR) << "Database connection lost";
            return false;
        }

        auto result = db_->execute(
            "SELECT is_online FROM vehicles WHERE vehicle_id = $1",
            {vehicle_id});

        if (result.ok() && result.num_rows() > 0) {
            std::string is_online = result.row(0).get_string("is_online");
            if (is_online == "t" || is_online == "true" || is_online == "1") {
                LOG(INFO) << "  Vehicle " << vehicle_id << " is online";
                return true;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    LOG(ERROR) << "Timeout waiting for vehicle " << vehicle_id << " to come online";
    return false;
}

bool E2ETestInfrastructure::WaitForServiceRegistered(const std::string& vehicle_id,
                                                      const std::string& service_name,
                                                      std::chrono::seconds timeout) {
    LOG(INFO) << "Waiting for service " << service_name << " on vehicle " << vehicle_id << "...";

    auto deadline = std::chrono::steady_clock::now() + timeout;

    while (std::chrono::steady_clock::now() < deadline) {
        if (!db_ || !db_->is_connected()) {
            LOG(ERROR) << "Database connection lost";
            return false;
        }

        // Check vehicle_schemas + schema_registry
        auto result = db_->execute(R"(
            SELECT COUNT(*) as cnt FROM vehicle_schemas vs
            JOIN schema_registry sr ON vs.schema_hash = sr.schema_hash
            WHERE vs.vehicle_id = $1 AND sr.service_name = $2
        )", {vehicle_id, service_name});

        if (result.ok() && result.num_rows() > 0) {
            int count = result.row(0).get_int("cnt");
            if (count > 0) {
                LOG(INFO) << "  Service " << service_name << " registered";
                return true;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    LOG(ERROR) << "Timeout waiting for service " << service_name << " to register";
    return false;
}

bool E2ETestInfrastructure::WaitForJobExecution(const std::string& job_id,
                                                 std::chrono::seconds timeout) {
    LOG(INFO) << "Waiting for job execution " << job_id << "...";

    auto deadline = std::chrono::steady_clock::now() + timeout;

    while (std::chrono::steady_clock::now() < deadline) {
        if (!db_ || !db_->is_connected()) {
            LOG(ERROR) << "Database connection lost";
            return false;
        }

        auto result = db_->execute(
            "SELECT COUNT(*) as cnt FROM job_executions WHERE job_id = $1",
            {job_id});

        if (result.ok() && result.num_rows() > 0) {
            int count = result.row(0).get_int("cnt");
            if (count > 0) {
                LOG(INFO) << "  Job execution found for " << job_id;
                return true;
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    LOG(ERROR) << "Timeout waiting for job execution " << job_id;
    return false;
}

void E2ETestInfrastructure::ResetDatabase(PostgresClient* external_db) {
    PostgresClient* db = external_db ? external_db : db_.get();
    if (!db || !db->is_connected()) {
        LOG(ERROR) << "Database not connected, cannot reset";
        return;
    }

    // Drop all tables and recreate schema
    // Order matters due to foreign key constraints
    const std::vector<std::string> drop_statements = {
        "DROP TABLE IF EXISTS job_executions CASCADE",
        "DROP TABLE IF EXISTS jobs CASCADE",
        "DROP TABLE IF EXISTS offboard_calendar CASCADE",
        "DROP TABLE IF EXISTS services CASCADE",
        "DROP TABLE IF EXISTS vehicle_schemas CASCADE",
        "DROP TABLE IF EXISTS schema_registry CASCADE",
        "DROP TABLE IF EXISTS sync_state CASCADE",
        "DROP TABLE IF EXISTS vehicle_enrichment CASCADE",
        "DROP TABLE IF EXISTS kafka_offsets CASCADE",
        "DROP TABLE IF EXISTS vehicles CASCADE",
        "DROP VIEW IF EXISTS vehicle_services_view CASCADE",
        "DROP VIEW IF EXISTS fleet_services_view CASCADE",
        "DROP FUNCTION IF EXISTS upsert_vehicle CASCADE",
        "DROP FUNCTION IF EXISTS update_sync_state CASCADE"
    };

    for (const auto& stmt : drop_statements) {
        auto result = db->execute(stmt, {});
        if (!result.ok()) {
            LOG(WARNING) << "Drop statement failed (may not exist): " << stmt;
        }
    }

    // Use CMake-configured path to schema.sql
    std::string schema_path = SCHEMA_SQL_PATH;
    LOG(INFO) << "Applying schema from: " << schema_path;

    std::string cmd = "PGPASSWORD=" + std::string(POSTGRES_PASSWORD) +
                      " psql -h " + POSTGRES_HOST +
                      " -p " + std::to_string(POSTGRES_PORT) +
                      " -U " + POSTGRES_USER +
                      " -d " + POSTGRES_DB +
                      " -f " + schema_path +
                      " 2>&1";

    FILE* pipe = popen(cmd.c_str(), "r");
    if (pipe) {
        char buffer[256];
        std::string output;
        while (fgets(buffer, sizeof(buffer), pipe)) {
            output += buffer;
        }
        int status = pclose(pipe);
        if (status != 0) {
            LOG(ERROR) << "psql schema.sql failed with status " << status;
            LOG(ERROR) << "Output: " << output;
        } else {
            LOG(INFO) << "Database schema recreated successfully";
        }
    } else {
        LOG(ERROR) << "Failed to run psql";
    }
}

}  // namespace ifex::offboard::test
