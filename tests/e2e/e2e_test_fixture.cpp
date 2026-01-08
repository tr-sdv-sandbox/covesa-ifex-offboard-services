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

bool E2ETestInfrastructure::StartDockerInfrastructure() {
    LOG(INFO) << "Starting E2E Docker infrastructure...";

    // Check if e2e-postgres is already running
    int ret = system("docker ps --format '{{.Names}}' | grep -q '^e2e-postgres$'");
    if (ret == 0) {
        LOG(INFO) << "  E2E Docker infrastructure already running";
        return true;
    }

    // Start the E2E Docker compose stack
    // Find the docker-compose.e2e.yml relative to test executable
    std::string test_dir = GetExecutableDir();
    // test_dir is like build/tests/e2e, we need source tests/e2e
    std::string compose_file = test_dir + "/../../tests/e2e/docker-compose.e2e.yml";

    // Try alternative paths
    if (system(("test -f " + compose_file).c_str()) != 0) {
        compose_file = "tests/e2e/docker-compose.e2e.yml";
    }
    if (system(("test -f " + compose_file).c_str()) != 0) {
        // Try from build directory
        compose_file = "../tests/e2e/docker-compose.e2e.yml";
    }

    // Try docker compose (v2) first, fall back to docker-compose (v1)
    std::string cmd = "docker compose -f " + compose_file + " up -d 2>&1 || docker-compose -f " + compose_file + " up -d 2>&1";
    LOG(INFO) << "  Running: " << cmd;

    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        LOG(ERROR) << "Failed to start E2E Docker infrastructure";
        return false;
    }

    char buffer[256];
    std::string output;
    while (fgets(buffer, sizeof(buffer), pipe)) {
        output += buffer;
    }
    int status = pclose(pipe);

    if (status != 0) {
        LOG(ERROR) << "docker compose up failed: " << output;
        return false;
    }

    LOG(INFO) << "  E2E Docker infrastructure started";

    // Wait for services to be healthy
    LOG(INFO) << "  Waiting for services to be ready...";
    std::this_thread::sleep_for(std::chrono::seconds(5));

    return true;
}

bool E2ETestInfrastructure::VerifyDockerInfrastructure() {
    LOG(INFO) << "Verifying Docker infrastructure...";

    // First, try to start E2E infrastructure if not running
    if (!StartDockerInfrastructure()) {
        LOG(ERROR) << "Failed to start E2E Docker infrastructure";
        LOG(ERROR) << "Run: docker compose -f tests/e2e/docker-compose.e2e.yml up -d";
        return false;
    }

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
        LOG(ERROR) << "Run: docker compose -f tests/e2e/docker-compose.e2e.yml up -d";
        return false;
    }
    LOG(INFO) << "  PostgreSQL: OK (" << POSTGRES_HOST << ":" << POSTGRES_PORT << ")";

    // Check MQTT (try to connect briefly)
    // We'll verify this when starting the bridge
    LOG(INFO) << "  MQTT endpoint: " << MQTT_HOST << ":" << MQTT_PORT;

    // Check Kafka (we'll verify when bridge starts)
    LOG(INFO) << "  Kafka endpoint: " << KAFKA_BROKER;

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
    LOG(INFO) << "Creating Kafka topics in E2E Kafka...";

    // Topics needed for bidirectional communication
    // Use e2e-prefixed topics to avoid conflicts with simulation
    const std::vector<std::string> topics = {
        "e2e.discovery.201",       // v2c: vehicle discovery sync
        "e2e.scheduler.202",       // v2c: vehicle scheduler sync
        "e2e.rpc.200",             // v2c: RPC responses
        "e2e.status",              // v2c: vehicle status
        "e2e.c2v.discovery",       // c2v: schema requests to vehicle
        "e2e.c2v.scheduler",       // c2v: scheduler commands to vehicle
        "e2e.c2v.rpc"              // c2v: RPC requests to vehicle
    };

    for (const auto& topic : topics) {
        // Use e2e-kafka container (from docker-compose.e2e.yml)
        // apache/kafka image uses /opt/kafka/bin/kafka-topics.sh
        // Internal port is 29092 (see KAFKA_ADVERTISED_LISTENERS)
        std::string cmd = "docker exec e2e-kafka /opt/kafka/bin/kafka-topics.sh "
                          "--bootstrap-server localhost:29092 "
                          "--create --if-not-exists "
                          "--topic " + topic + " "
                          "--partitions 1 "
                          "--replication-factor 1 "
                          ">/dev/null 2>&1";
        system(cmd.c_str());
    }

    LOG(INFO) << "  Created " << topics.size() << " Kafka topics (e2e.* prefix)";
    return true;
}

bool E2ETestInfrastructure::StartBridgeServices() {
    LOG(INFO) << "Starting bridge services (E2E isolated)...";

    std::string bin_dir = GetBinaryPath("");

    // Convert port to string for execl
    std::string mqtt_port_str = std::to_string(MQTT_PORT);
    std::string postgres_port_str = std::to_string(POSTGRES_PORT);

    // Start mqtt_kafka_bridge with E2E-specific settings
    // Use e2e.* topic names for isolation from simulation
    mqtt_kafka_bridge_pid_ = fork();
    if (mqtt_kafka_bridge_pid_ == 0) {
        std::string binary = bin_dir + "mqtt_kafka_bridge";
        execl(binary.c_str(), "mqtt_kafka_bridge",
              "--mqtt_host", MQTT_HOST,
              "--mqtt_port", mqtt_port_str.c_str(),
              "--kafka_broker", KAFKA_BROKER,
              "--postgres_host", POSTGRES_HOST,
              "--postgres_port", postgres_port_str.c_str(),
              // E2E-specific Kafka topics
              "--kafka_topic_rpc", "e2e.rpc.200",
              "--kafka_topic_discovery", "e2e.discovery.201",
              "--kafka_topic_scheduler", "e2e.scheduler.202",
              "--kafka_topic_status", "e2e.status",
              "--kafka_topic_c2v_rpc", "e2e.c2v.rpc",
              "--kafka_topic_c2v_discovery", "e2e.c2v.discovery",
              "--kafka_topic_c2v_scheduler", "e2e.c2v.scheduler",
              "--logtostderr",
              nullptr);
        LOG(ERROR) << "Failed to exec mqtt_kafka_bridge";
        _exit(1);
    }
    if (mqtt_kafka_bridge_pid_ < 0) {
        LOG(ERROR) << "Failed to fork mqtt_kafka_bridge";
        return false;
    }
    LOG(INFO) << "  mqtt_kafka_bridge: PID " << mqtt_kafka_bridge_pid_
              << " (MQTT=" << MQTT_HOST << ":" << MQTT_PORT
              << ", Kafka=" << KAFKA_BROKER << ")";

    // Start discovery_mirror with E2E-specific settings
    discovery_mirror_pid_ = fork();
    if (discovery_mirror_pid_ == 0) {
        std::string binary = bin_dir + "discovery_mirror";
        execl(binary.c_str(), "discovery_mirror",
              "--kafka_broker", KAFKA_BROKER,
              "--postgres_host", POSTGRES_HOST,
              "--postgres_port", postgres_port_str.c_str(),
              "--kafka_topic", "e2e.discovery.201",
              "--kafka_topic_c2v", "e2e.c2v.discovery",
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

    // Start scheduler_mirror with E2E-specific settings
    scheduler_mirror_pid_ = fork();
    if (scheduler_mirror_pid_ == 0) {
        std::string binary = bin_dir + "scheduler_mirror";
        execl(binary.c_str(), "scheduler_mirror",
              "--kafka_broker", KAFKA_BROKER,
              "--postgres_host", POSTGRES_HOST,
              "--postgres_port", postgres_port_str.c_str(),
              "--kafka_topic", "e2e.scheduler.202",
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
    LOG(INFO) << "Starting vehicle container (E2E isolated)...";

    // Stop any existing container with same name
    system(("docker rm -f " + std::string(VEHICLE_CONTAINER_NAME) + " >/dev/null 2>&1").c_str());

    // E2E network should already exist from docker-compose.e2e.yml
    // No need to create it manually

    // Start vehicle container on E2E test network
    // Vehicle connects to e2e-mosquitto (internal Docker DNS)
    // MQTT port inside container is always 1883 (e2e-mosquitto exposes 1883 internally)
    std::string cmd = "docker run -d"
                      " --name " + std::string(VEHICLE_CONTAINER_NAME) +
                      " --network " + std::string(DOCKER_NETWORK) +
                      " -e VEHICLE_ID=" + std::string(VEHICLE_ID) +
                      " -e MQTT_HOST=" + std::string(MQTT_HOST_DOCKER) +
                      " -e MQTT_PORT=1883"  // Internal port in e2e-mosquitto
                      " -e KAFKA_BROKER=" + std::string(KAFKA_BROKER_DOCKER) +
                      " -e START_TEST_SERVICES=true"
                      " " + std::string(VEHICLE_IMAGE);

    LOG(INFO) << "  Command: " << cmd;

    int ret = system(cmd.c_str());
    if (ret != 0) {
        LOG(ERROR) << "Failed to start vehicle container";
        return false;
    }

    LOG(INFO) << "  Vehicle container started: " << VEHICLE_CONTAINER_NAME
              << " (network=" << DOCKER_NETWORK
              << ", mqtt=" << MQTT_HOST_DOCKER << ":1883)";
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
