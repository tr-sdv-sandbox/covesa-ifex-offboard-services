#!/bin/bash
# Start IFEX Offboard Infrastructure
# Usage: ./start-infra.sh [num_vehicles]
#
# This script:
# 1. Starts Docker infrastructure (Kafka, PostgreSQL, Mosquitto)
# 2. Waits for services to be healthy
# 3. Populates test data in PostgreSQL
# 4. Exports enrichment data to Kafka
# 5. Starts the mirror services (discovery, scheduler, rpc)
# 6. Starts the MQTT-Kafka bridge

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_VEHICLES="${1:-100000}"
BUILD_DIR="../build"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check build directory
if [ ! -d "$BUILD_DIR" ]; then
    log_error "Build directory not found: $BUILD_DIR"
    log_error "Run: mkdir build && cd build && cmake .. && make -j"
    exit 1
fi

# Check required binaries
REQUIRED_BINS="mqtt_kafka_bridge discovery_mirror scheduler_mirror enrichment_exporter dispatcher_api discovery_api scheduler_api"
for bin in $REQUIRED_BINS; do
    if [ ! -x "$BUILD_DIR/$bin" ]; then
        log_error "Binary not found: $BUILD_DIR/$bin"
        exit 1
    fi
done

log_info "Starting IFEX Offboard Infrastructure with $NUM_VEHICLES vehicles"

# Step 1: Start Docker infrastructure
log_info "Starting Docker containers..."
docker-compose up -d

# Step 2: Wait for services
log_info "Waiting for PostgreSQL..."
for i in {1..30}; do
    if docker-compose exec -T postgres pg_isready -U ifex >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

log_info "Waiting for Kafka..."
for i in {1..30}; do
    if docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Step 3: Create Kafka topics
log_info "Setting up Kafka topics..."

# Create standard topics (if not exists)
for topic in ifex.rpc.200 ifex.discovery.201 ifex.scheduler.202 ifex.c2v.rpc ifex.c2v.discovery ifex.c2v.scheduler ifex.status; do
    docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:29092 --create --if-not-exists --topic $topic \
        --partitions 3 --replication-factor 1 2>/dev/null || true
done

# Enrichment topic needs compaction - recreate it fresh
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --delete --topic ifex.vehicle.enrichment 2>/dev/null || true
sleep 1
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --create --topic ifex.vehicle.enrichment \
    --partitions 3 --replication-factor 1 \
    --config cleanup.policy=compact 2>/dev/null || true

log_info "Kafka topics created"

# Step 4: Populate PostgreSQL with test data
log_info "Populating PostgreSQL with $NUM_VEHICLES vehicles..."
docker-compose exec -T postgres psql -U ifex -d ifex_offboard -q <<EOF
TRUNCATE vehicles, vehicle_enrichment, vehicle_schemas, schema_registry, jobs, job_executions, sync_state, offboard_calendar CASCADE;

CREATE OR REPLACE FUNCTION generate_vin(seq INTEGER) RETURNS VARCHAR AS \$\$
BEGIN
    RETURN UPPER('VIN' || LPAD(seq::TEXT, 14, '0'));
END;
\$\$ LANGUAGE plpgsql;

INSERT INTO vehicles (vehicle_id, first_seen_at, is_online)
SELECT generate_vin(n), NOW(), false
FROM generate_series(1, $NUM_VEHICLES) as n;

INSERT INTO vehicle_enrichment (vehicle_id, fleet_id, region, model, year, owner, tags)
SELECT
    generate_vin(n),
    'fleet-' || LPAD((1 + (n % 100))::TEXT, 3, '0'),
    (ARRAY['eu-west', 'eu-central', 'eu-north', 'us-east', 'us-west', 'apac'])[1 + (n % 6)],
    (ARRAY['TGX', 'TGS', 'TGM', 'TGL', 'MAN Lion', 'Scania R', 'Scania S'])[1 + (n % 7)],
    2020 + (n % 5),
    'Operator ' || (1 + (n % 100)),
    '[]'::jsonb
FROM generate_series(1, $NUM_VEHICLES) as n;

DROP FUNCTION generate_vin(INTEGER);
EOF
log_info "PostgreSQL populated"

# Step 5: Export enrichment to Kafka
log_info "Exporting enrichment data to Kafka..."
timeout 120 "$BUILD_DIR/enrichment_exporter" \
    --kafka_broker=localhost:9092 \
    --full_export_on_start=true \
    --poll_interval_sec=1 2>&1 | while read line; do
    echo "$line"
    if echo "$line" | grep -q "Full export complete"; then
        pkill -f enrichment_exporter || true
        break
    fi
done
log_info "Enrichment export complete"

# Step 6: Start mirror services in background
log_info "Starting mirror services..."

mkdir -p /tmp/ifex-logs /tmp/ifex-pids

"$BUILD_DIR/discovery_mirror" \
    --kafka_broker=localhost:9092 \
    --postgres_host=localhost \
    > /tmp/ifex-logs/discovery_mirror.log 2>&1 &
echo $! > /tmp/ifex-pids/discovery_mirror.pid
log_info "  discovery_mirror started (PID: $!)"

"$BUILD_DIR/scheduler_mirror" \
    --kafka_broker=localhost:9092 \
    --postgres_host=localhost \
    > /tmp/ifex-logs/scheduler_mirror.log 2>&1 &
echo $! > /tmp/ifex-pids/scheduler_mirror.pid
log_info "  scheduler_mirror started (PID: $!)"

# Step 7: Start Cloud API services
log_info "Starting Cloud API services..."

"$BUILD_DIR/dispatcher_api" \
    --kafka_broker=localhost:9092 \
    --mqtt_host=localhost \
    --postgres_host=localhost \
    --listen=0.0.0.0:50100 \
    > /tmp/ifex-logs/dispatcher_api.log 2>&1 &
echo $! > /tmp/ifex-pids/dispatcher_api.pid
log_info "  dispatcher_api started (PID: $!, port 50100)"

"$BUILD_DIR/discovery_api" \
    --postgres_host=localhost \
    --listen=0.0.0.0:50101 \
    > /tmp/ifex-logs/discovery_api.log 2>&1 &
echo $! > /tmp/ifex-pids/discovery_api.pid
log_info "  discovery_api started (PID: $!, port 50101)"

"$BUILD_DIR/scheduler_api" \
    --kafka_broker=localhost:9092 \
    --mqtt_host=localhost \
    --postgres_host=localhost \
    --listen=0.0.0.0:50102 \
    > /tmp/ifex-logs/scheduler_api.log 2>&1 &
echo $! > /tmp/ifex-pids/scheduler_api.pid
log_info "  scheduler_api started (PID: $!, port 50102)"

# Step 8: Start MQTT-Kafka bridge
log_info "Starting MQTT-Kafka bridge..."
"$BUILD_DIR/mqtt_kafka_bridge" \
    --kafka_broker=localhost:9092 \
    --mqtt_host=localhost \
    > /tmp/ifex-logs/mqtt_kafka_bridge.log 2>&1 &
echo $! > /tmp/ifex-pids/mqtt_kafka_bridge.pid
log_info "  mqtt_kafka_bridge started (PID: $!)"

# Wait for bridge to load enrichment
sleep 3
if grep -q "Initial enrichment load complete" /tmp/ifex-logs/mqtt_kafka_bridge.log 2>/dev/null; then
    STATS=$(grep "Initial enrichment load complete" /tmp/ifex-logs/mqtt_kafka_bridge.log | tail -1)
    log_info "Bridge ready: $STATS"
else
    log_warn "Bridge still loading enrichment data..."
fi

echo ""
log_info "=== Infrastructure Ready ==="
echo ""
echo "Services:"
echo "  - PostgreSQL:        localhost:5432 (ifex_offboard)"
echo "  - Kafka:             localhost:9092"
echo "  - Mosquitto:         localhost:1883"
echo "  - Fleet Dashboard:   http://localhost:5000/"
echo ""
echo "Cloud APIs (gRPC):"
echo "  - Dispatcher API:    localhost:50100"
echo "  - Discovery API:     localhost:50101"
echo "  - Scheduler API:     localhost:50102"
echo ""
echo "Background Services:"
echo "  - MQTT-Kafka Bridge: running"
echo "  - Discovery Mirror:  running"
echo "  - Scheduler Mirror:  running"
echo ""
echo "Logs: /tmp/ifex-logs/"
echo "Stop: ./stop-infra.sh"
echo ""
