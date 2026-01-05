#!/bin/bash
# Test mqtt_kafka_bridge startup time for loading enrichment data
# Usage: ./test-bridge-startup.sh [timeout_seconds]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TIMEOUT="${1:-60}"

echo "=== Testing Bridge Startup ==="

# Get record count from Kafka
COUNT=$(docker-compose exec -T kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:29092 --topic ifex.vehicle.enrichment 2>/dev/null | \
    awk -F: '{sum += $3} END {print sum}' || echo "unknown")
echo "Enrichment records in Kafka: $COUNT"
echo ""

# Run bridge and capture timing
echo "Starting bridge (timeout: ${TIMEOUT}s)..."
START_TIME=$(date +%s.%N)

timeout $TIMEOUT ../build/mqtt_kafka_bridge \
    --kafka_broker=localhost:9092 \
    --enrichment_load_timeout_s=$TIMEOUT 2>&1 | tee /tmp/bridge_output.log &
BRIDGE_PID=$!

# Wait for "Initial enrichment load complete" or timeout
while kill -0 $BRIDGE_PID 2>/dev/null; do
    if grep -q "Initial enrichment load complete" /tmp/bridge_output.log 2>/dev/null; then
        END_TIME=$(date +%s.%N)
        LOAD_TIME=$(echo "$END_TIME - $START_TIME" | bc)

        # Extract stats from log
        LOADED=$(grep "Initial enrichment load complete" /tmp/bridge_output.log | grep -oP '\d+ records' | head -1)
        VEHICLES=$(grep "unique vehicles" /tmp/bridge_output.log | grep -oP '\d+ unique' | head -1)

        echo ""
        echo "=== Results ==="
        echo "Load time:    ${LOAD_TIME}s"
        echo "Records:      $LOADED"
        echo "Vehicles:     $VEHICLES"

        # Calculate throughput
        RECORD_COUNT=$(echo "$LOADED" | grep -oP '\d+')
        if [ -n "$RECORD_COUNT" ] && [ "$RECORD_COUNT" -gt 0 ]; then
            THROUGHPUT=$(echo "scale=0; $RECORD_COUNT / $LOAD_TIME" | bc)
            echo "Throughput:   $THROUGHPUT records/sec"
        fi

        kill $BRIDGE_PID 2>/dev/null || true
        break
    fi
    sleep 0.1
done

wait $BRIDGE_PID 2>/dev/null || true
rm -f /tmp/bridge_output.log
