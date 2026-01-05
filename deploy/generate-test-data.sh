#!/bin/bash
# Generate test data for IFEX Offboard Services
# Usage: ./generate-test-data.sh [num_vehicles]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NUM_VEHICLES="${1:-1000}"

echo "=== Generating $NUM_VEHICLES test vehicles ==="

# Step 1: Reset database and insert data
echo "Inserting into PostgreSQL..."
START_DB=$(date +%s.%N)

docker-compose exec -T postgres psql -U ifex -d ifex_offboard -q <<EOF
TRUNCATE vehicles, vehicle_enrichment, services, jobs, job_executions, rpc_requests, sync_state CASCADE;

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

END_DB=$(date +%s.%N)
echo "PostgreSQL: $(echo "$END_DB - $START_DB" | bc)s"

# Step 2: Reset Kafka topic
echo "Resetting Kafka topic..."
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --delete --topic ifex.vehicle.enrichment 2>/dev/null || true
sleep 1
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --create --topic ifex.vehicle.enrichment \
    --partitions 3 --replication-factor 1 \
    --config cleanup.policy=compact 2>/dev/null

# Step 3: Export to Kafka
echo "Exporting to Kafka..."
START_KAFKA=$(date +%s.%N)

timeout 600 ../build/enrichment_exporter \
    --kafka_broker=localhost:9092 \
    --full_export_on_start=true \
    --poll_interval_sec=1 2>&1 | while read line; do
    echo "$line"
    if echo "$line" | grep -q "Full export complete"; then
        pkill -f enrichment_exporter || true
        break
    fi
done

END_KAFKA=$(date +%s.%N)
echo "Kafka export: $(echo "$END_KAFKA - $START_KAFKA" | bc)s"

echo ""
echo "=== Done: $NUM_VEHICLES vehicles ==="
