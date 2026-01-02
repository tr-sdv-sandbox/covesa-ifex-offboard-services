#!/bin/bash
# Start development infrastructure

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Starting IFEX Offboard development infrastructure..."

cd "$PROJECT_DIR/docker"

# Start core services
docker compose up -d

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
for i in {1..30}; do
    if docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
        echo "Kafka is ready!"
        break
    fi
    echo -n "."
    sleep 2
done

# Show status
echo ""
echo "Services:"
docker compose ps

echo ""
echo "Kafka topics:"
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list

echo ""
echo "PostgreSQL tables:"
docker compose exec -T postgres psql -U ifex ifex_offboard -c "\\dt"

echo ""
echo "Infrastructure ready!"
echo ""
echo "Next steps:"
echo "  1. Build: cd ../build && cmake .. && make -j"
echo "  2. Run bridge: ./mqtt_kafka_bridge"
echo "  3. Run mirrors: ./discovery_mirror, ./scheduler_mirror, ./rpc_gateway"
echo ""
echo "Debugging:"
echo "  - Kafka UI: docker compose --profile debug up -d (http://localhost:8080)"
echo "  - PostgreSQL: psql -h localhost -U ifex ifex_offboard"
echo "  - MQTT: mosquitto_sub -h localhost -t 'v2c/#' -v"
