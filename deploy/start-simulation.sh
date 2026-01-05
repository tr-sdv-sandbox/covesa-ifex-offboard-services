#!/bin/bash
# Start Full IFEX Simulation Environment
# Usage: ./start-simulation.sh [num_vehicles] [options]
#
# This script:
# 1. Starts infrastructure (Kafka, PostgreSQL, Mosquitto)
# 2. Populates enrichment data for N vehicles
# 3. Starts offboard services (bridge, mirrors)
# 4. Starts N simulated vehicle containers with matching VINs
#
# Examples:
#   ./start-simulation.sh              # 10 vehicles (default)
#   ./start-simulation.sh 5            # 5 vehicles
#   ./start-simulation.sh 20 --clean   # 20 vehicles, rebuild everything

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Defaults
NUM_VEHICLES="${1:-10}"
VEHICLE_IMAGE="ifex-vehicle:latest"
VIN_PREFIX="VIN"
NETWORK_NAME="ifex-simulation"

# Parse options
shift || true
CLEAN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN=true
            shift
            ;;
        --vin-prefix)
            VIN_PREFIX="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

log_info "=== Starting IFEX Simulation Environment ==="
log_info "Vehicles: $NUM_VEHICLES"
log_info "VIN Pattern: ${VIN_PREFIX}00000000000001 - ${VIN_PREFIX}$(printf '%014d' $NUM_VEHICLES)"

# Step 1: Check vehicle image exists
if ! docker image inspect "$VEHICLE_IMAGE" >/dev/null 2>&1; then
    log_warn "Vehicle image not found: $VEHICLE_IMAGE"
    log_warn "Build it from covesa-ifex-core:"
    log_warn "  cd ../covesa-ifex-core && ./deploy/build-image.sh"
    exit 1
fi

# Step 2: Stop any existing simulation
log_info "Stopping any existing simulation..."
./stop-simulation.sh 2>/dev/null || true

# Step 3: Create Docker network
if ! docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
    log_info "Creating Docker network: $NETWORK_NAME"
    docker network create "$NETWORK_NAME"
fi

# Step 4: Start infrastructure with matching vehicle count
log_info "Starting infrastructure with $NUM_VEHICLES vehicles worth of enrichment data..."
./start-infra.sh "$NUM_VEHICLES"

# Step 4b: Connect infrastructure containers to simulation network (after they're created)
log_info "Connecting infrastructure to simulation network..."
docker network connect "$NETWORK_NAME" ifex-kafka 2>/dev/null || true
docker network connect "$NETWORK_NAME" ifex-postgres 2>/dev/null || true
docker network connect "$NETWORK_NAME" ifex-mosquitto 2>/dev/null || true

# Step 5: Start vehicle containers
log_info "Starting $NUM_VEHICLES vehicle containers..."

mkdir -p /tmp/ifex-simulation

for i in $(seq 1 $NUM_VEHICLES); do
    # Generate VIN matching test data format (14 digits after prefix)
    VIN=$(printf "%s%014d" "$VIN_PREFIX" "$i")
    CONTAINER_NAME="ifex-vehicle-$i"

    log_info "  Starting vehicle $i: $VIN"

    docker run -d \
        --name "$CONTAINER_NAME" \
        --network "$NETWORK_NAME" \
        -e VEHICLE_ID="$VIN" \
        -e MQTT_HOST="ifex-mosquitto" \
        -e MQTT_PORT="1883" \
        -e START_TEST_SERVICES=true \
        "$VEHICLE_IMAGE" >/dev/null

    # Small delay to avoid overwhelming the broker
    sleep 0.3
done

# Step 6: Wait for vehicles to register
log_info "Waiting for vehicles to register services..."
sleep 5

# Step 7: Verify simulation
log_info "Verifying simulation..."

# Count registered services
SERVICE_COUNT=$(docker-compose exec -T postgres psql -U ifex -d ifex_offboard -t -c \
    "SELECT COUNT(DISTINCT vehicle_id) FROM services;" 2>/dev/null | tr -d ' ')

# Count vehicles with enrichment
ENRICHED_COUNT=$(docker-compose exec -T postgres psql -U ifex -d ifex_offboard -t -c \
    "SELECT COUNT(*) FROM vehicle_enrichment WHERE vehicle_id IN (SELECT DISTINCT vehicle_id FROM services);" 2>/dev/null | tr -d ' ')

echo ""
log_info "=== Simulation Ready ==="
echo ""
echo "Infrastructure:"
echo "  - PostgreSQL:    localhost:5432"
echo "  - Kafka:         localhost:9092"
echo "  - Mosquitto:     localhost:1883"
echo ""
echo "Offboard Services:"
echo "  - MQTT-Kafka Bridge:  running"
echo "  - Discovery Mirror:   running"
echo "  - Scheduler Mirror:   running"
echo "  - RPC Gateway:        running"
echo ""
echo "Vehicles:"
echo "  - Containers:    $NUM_VEHICLES"
echo "  - With services: $SERVICE_COUNT"
echo "  - With enrichment: $ENRICHED_COUNT"
echo ""
echo "VIN Range: ${VIN_PREFIX}00000000000001 - ${VIN_PREFIX}$(printf '%014d' $NUM_VEHICLES)"
echo ""
echo "Commands:"
echo "  View logs:     docker logs -f ifex-vehicle-1"
echo "  Query DB:      docker-compose exec postgres psql -U ifex -d ifex_offboard"
echo "  Monitor MQTT:  mosquitto_sub -h localhost -t 'v2c/#' -v"
echo "  Stop:          ./stop-simulation.sh"
echo ""
