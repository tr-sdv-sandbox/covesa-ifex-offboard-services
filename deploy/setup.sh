#!/bin/bash
# Setup script for IFEX Offboard Services test environment
# Starts PostgreSQL, Kafka, and MQTT with seed data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse arguments
WITH_UI=false
FORCE_RECREATE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --with-ui)
            WITH_UI=true
            shift
            ;;
        --recreate)
            FORCE_RECREATE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --with-ui      Also start Kafka UI and pgAdmin"
            echo "  --recreate     Force recreate containers and volumes"
            echo "  --help         Show this help message"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check for docker
if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
fi

# Check for docker-compose
if ! docker-compose version &> /dev/null; then
    log_error "Docker Compose is not installed"
    exit 1
fi

# Recreate if requested
if [ "$FORCE_RECREATE" = true ]; then
    log_info "Stopping and removing existing containers..."
    docker-compose down -v 2>/dev/null || true
fi

# Start services
log_info "Starting IFEX Offboard test environment..."

if [ "$WITH_UI" = true ]; then
    docker-compose --profile ui up -d
else
    docker-compose up -d
fi

# Wait for PostgreSQL to be ready
log_info "Waiting for PostgreSQL to be ready..."
RETRIES=30
until docker-compose exec -T postgres pg_isready -U ifex -d ifex_offboard > /dev/null 2>&1; do
    RETRIES=$((RETRIES - 1))
    if [ $RETRIES -le 0 ]; then
        log_error "PostgreSQL did not become ready in time"
        exit 1
    fi
    sleep 1
done

# Wait for Kafka to be ready (apache/kafka image uses /opt/kafka/bin/)
log_info "Waiting for Kafka to be ready..."
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
RETRIES=30
until docker-compose exec -T kafka $KAFKA_TOPICS --bootstrap-server localhost:29092 --list > /dev/null 2>&1; do
    RETRIES=$((RETRIES - 1))
    if [ $RETRIES -le 0 ]; then
        log_error "Kafka did not become ready in time"
        exit 1
    fi
    sleep 1
done

# Create Kafka topics
log_info "Creating Kafka topics..."
docker-compose exec -T kafka $KAFKA_TOPICS --bootstrap-server localhost:29092 \
    --create --if-not-exists --topic ifex.rpc.200 --partitions 3 --replication-factor 1

docker-compose exec -T kafka $KAFKA_TOPICS --bootstrap-server localhost:29092 \
    --create --if-not-exists --topic ifex.discovery.201 --partitions 3 --replication-factor 1

docker-compose exec -T kafka $KAFKA_TOPICS --bootstrap-server localhost:29092 \
    --create --if-not-exists --topic ifex.scheduler.202 --partitions 3 --replication-factor 1

# Create compacted topic for vehicle enrichment data
# Compaction ensures consumers always get latest state per vehicle_id key
docker-compose exec -T kafka $KAFKA_TOPICS --bootstrap-server localhost:29092 \
    --create --if-not-exists --topic ifex.vehicle.enrichment \
    --partitions 3 --replication-factor 1 \
    --config cleanup.policy=compact \
    --config min.cleanable.dirty.ratio=0.1 \
    --config segment.ms=100

# Print summary
echo ""
log_info "Test environment is ready!"
echo ""
echo "Services:"
echo "  PostgreSQL:  localhost:5432  (user: ifex, password: ifex_dev, db: ifex_offboard)"
echo "  Kafka:       localhost:9092"
echo "  MQTT:        localhost:1883"
if [ "$WITH_UI" = true ]; then
    echo "  Kafka UI:    http://localhost:8080"
    echo "  pgAdmin:     http://localhost:5050 (admin@ifex.local / admin)"
fi
echo ""
echo "Database contains:"
docker-compose exec -T postgres psql -U ifex -d ifex_offboard -t -c \
    "SELECT 'Vehicles: ' || COUNT(*) FROM vehicles
     UNION ALL SELECT 'Enrichment: ' || COUNT(*) FROM vehicle_enrichment
     UNION ALL SELECT 'Services: ' || COUNT(*) FROM services
     UNION ALL SELECT 'Jobs: ' || COUNT(*) FROM jobs
     UNION ALL SELECT 'Job Executions: ' || COUNT(*) FROM job_executions
     UNION ALL SELECT 'RPC Requests: ' || COUNT(*) FROM rpc_requests;"
echo ""
echo "To stop:   docker-compose -f $SCRIPT_DIR/docker-compose.yml down"
echo "To reset:  $0 --recreate"
