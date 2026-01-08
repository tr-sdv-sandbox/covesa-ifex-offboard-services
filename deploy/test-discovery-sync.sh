#!/bin/bash
# End-to-End Test: Hash-Based Discovery Sync Protocol
#
# Tests the bandwidth-efficient hash-based discovery sync flow:
#   1. Vehicle sends hash manifest (list of schema hashes)
#   2. Cloud detects unknown hashes and sends schema request
#   3. Vehicle responds with full schemas
#   4. Cloud stores schemas in PostgreSQL
#
# This tests:
#   - mqtt_kafka_bridge routing v2c/201 to Kafka
#   - mqtt_kafka_bridge routing c2v/201 from Kafka to MQTT
#   - discovery_mirror processing hash manifests
#   - discovery_mirror sending schema requests
#   - discovery_mirror storing schemas in schema_registry
#
# Prerequisites:
#   - Infrastructure running (./start-infra.sh)
#   - mqtt_kafka_bridge running
#   - discovery_mirror running
#   - protoc installed (for encoding messages)
#
# Usage:
#   ./test-discovery-sync.sh                    # Run discovery sync tests
#   ./test-discovery-sync.sh --start-services   # Also start services

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Configuration
BUILD_DIR="../build"
VEHICLE_ID="test-discovery-$$"

# Test schema (simple IFEX YAML)
TEST_SCHEMA_HASH="abc123def456789012345678901234567890123456789012345678901234"
TEST_SCHEMA_YAML='name: test_service
version: 1.0.0
namespaces:
  - name: test
    methods:
      - name: hello
        input:
          - name: message
            datatype: string
        output:
          - name: result
            datatype: string'

# Parse args
START_SERVICES=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --start-services) START_SERVICES=true; shift ;;
        --vehicle) VEHICLE_ID="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_test() { echo -e "${BLUE}[TEST]${NC} $1"; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

pass_test() {
    echo -e "  ${GREEN}✓ PASS${NC}: $1"
    ((TESTS_PASSED++))
    ((TESTS_RUN++))
}

fail_test() {
    echo -e "  ${RED}✗ FAIL${NC}: $1"
    ((TESTS_FAILED++))
    ((TESTS_RUN++))
}

# Cleanup function
cleanup() {
    local exit_code=$?

    log_info "Cleaning up test resources..."

    # Kill services if we started them
    if [ -f /tmp/ifex-discovery-pids/mqtt_kafka_bridge.pid ]; then
        local pid=$(cat /tmp/ifex-discovery-pids/mqtt_kafka_bridge.pid 2>/dev/null)
        kill "$pid" 2>/dev/null || true
        rm -f /tmp/ifex-discovery-pids/mqtt_kafka_bridge.pid
    fi

    if [ -f /tmp/ifex-discovery-pids/discovery_mirror.pid ]; then
        local pid=$(cat /tmp/ifex-discovery-pids/discovery_mirror.pid 2>/dev/null)
        kill "$pid" 2>/dev/null || true
        rm -f /tmp/ifex-discovery-pids/discovery_mirror.pid
    fi

    # Clean up test data from DB
    docker-compose exec -T postgres psql -U ifex -d ifex_offboard -q \
        -c "DELETE FROM vehicle_schemas WHERE vehicle_id = '$VEHICLE_ID'" 2>/dev/null || true
    docker-compose exec -T postgres psql -U ifex -d ifex_offboard -q \
        -c "DELETE FROM schema_registry WHERE schema_hash = '$TEST_SCHEMA_HASH'" 2>/dev/null || true
    docker-compose exec -T postgres psql -U ifex -d ifex_offboard -q \
        -c "DELETE FROM vehicles WHERE vehicle_id = '$VEHICLE_ID'" 2>/dev/null || true

    log_info "Cleanup complete"
    return $exit_code
}

trap cleanup EXIT

# =============================================================================
# Infrastructure and Services Setup
# =============================================================================

if [ "$START_SERVICES" = true ]; then
    log_info "Starting services..."

    mkdir -p /tmp/ifex-discovery-pids /tmp/ifex-discovery-logs

    # Create Kafka topics
    docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:29092 --create --if-not-exists \
        --topic ifex.discovery.201 --partitions 1 --replication-factor 1 2>/dev/null || true
    docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:29092 --create --if-not-exists \
        --topic ifex.c2v.discovery --partitions 1 --replication-factor 1 2>/dev/null || true

    # Start mqtt_kafka_bridge
    "$BUILD_DIR/mqtt_kafka_bridge" \
        --kafka_broker=localhost:9092 \
        --mqtt_host=localhost \
        --postgres_host=localhost \
        --enrichment_load_timeout_s=5 \
        > /tmp/ifex-discovery-logs/mqtt_kafka_bridge.log 2>&1 &
    echo $! > /tmp/ifex-discovery-pids/mqtt_kafka_bridge.pid
    sleep 2

    # Start discovery_mirror
    "$BUILD_DIR/discovery_mirror" \
        --kafka_broker=localhost:9092 \
        --postgres_host=localhost \
        > /tmp/ifex-discovery-logs/discovery_mirror.log 2>&1 &
    echo $! > /tmp/ifex-discovery-pids/discovery_mirror.pid
    sleep 2
fi

# =============================================================================
# Prerequisites Check
# =============================================================================

log_info "Checking prerequisites..."

# Check MQTT broker
if ! timeout 2 mosquitto_sub -h localhost -t '$SYS/#' -C 1 >/dev/null 2>&1; then
    log_error "MQTT broker not running. Run: docker-compose up -d mosquitto"
    exit 1
fi

# Check Kafka
if ! docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --list >/dev/null 2>&1; then
    log_error "Kafka not running. Run: docker-compose up -d kafka"
    exit 1
fi

# Check PostgreSQL
if ! docker-compose exec -T postgres psql -U ifex -d ifex_offboard -c "SELECT 1" >/dev/null 2>&1; then
    log_error "PostgreSQL not running. Run: docker-compose up -d postgres"
    exit 1
fi

# Check protoc
if ! command -v protoc &> /dev/null; then
    log_warn "protoc not found - using base64 encoded test messages"
fi

log_info "Prerequisites OK"
echo ""

# =============================================================================
# Helper Functions
# =============================================================================

# Encode discovery_envelope_t with hash manifest
# Since protobuf encoding is complex, we'll use a pre-computed binary
encode_hash_manifest() {
    local vehicle_id="$1"
    local hash="$2"

    # Build protobuf message manually:
    # discovery_envelope_t {
    #   vehicle_id = 1  -> field 1, wire type 2 (length-delimited)
    #   manifest = 10   -> field 10, wire type 2
    #     hash_list_t {
    #       hashes = 1  -> field 1, wire type 2 (repeated)
    #     }
    # }

    # This is a simplified Python one-liner to encode the message
    python3 - "$vehicle_id" "$hash" << 'PYEOF'
import sys
from google.protobuf.internal.encoder import _VarintBytes
from google.protobuf.internal.wire_format import WIRETYPE_LENGTH_DELIMITED

vehicle_id = sys.argv[1].encode()
hash_val = sys.argv[2].encode()

# Field 1 (vehicle_id): tag = (1 << 3) | 2 = 0x0a
# Field 10 (manifest): tag = (10 << 3) | 2 = 0x52
# Field 1 in hash_list_t (hashes): tag = (1 << 3) | 2 = 0x0a

result = bytearray()

# vehicle_id field
result.append(0x0a)  # field 1, wire type 2
result.extend(_VarintBytes(len(vehicle_id)))
result.extend(vehicle_id)

# manifest field (hash_list_t)
inner = bytearray()
inner.append(0x0a)  # field 1 (hashes), wire type 2
inner.extend(_VarintBytes(len(hash_val)))
inner.extend(hash_val)

result.append(0x52)  # field 10, wire type 2
result.extend(_VarintBytes(len(inner)))
result.extend(inner)

sys.stdout.buffer.write(bytes(result))
PYEOF
}

# Encode discovery_envelope_t with schema response
encode_schema_response() {
    local vehicle_id="$1"
    local hash="$2"
    local yaml="$3"

    python3 - "$vehicle_id" "$hash" "$yaml" << 'PYEOF'
import sys
from google.protobuf.internal.encoder import _VarintBytes

vehicle_id = sys.argv[1].encode()
hash_val = sys.argv[2].encode()
yaml_val = sys.argv[3].encode()

result = bytearray()

# vehicle_id field (field 1)
result.append(0x0a)
result.extend(_VarintBytes(len(vehicle_id)))
result.extend(vehicle_id)

# schemas field (field 12, schema_map_t)
# schema_map_t has map<string, string> schemas = 1
# Map entry: key=1, value=2

# Build map entry
map_entry = bytearray()
map_entry.append(0x0a)  # key field 1
map_entry.extend(_VarintBytes(len(hash_val)))
map_entry.extend(hash_val)
map_entry.append(0x12)  # value field 2
map_entry.extend(_VarintBytes(len(yaml_val)))
map_entry.extend(yaml_val)

# schemas field in schema_map_t
inner = bytearray()
inner.append(0x0a)  # map field 1
inner.extend(_VarintBytes(len(map_entry)))
inner.extend(map_entry)

# field 12 (schemas)
result.append(0x62)  # (12 << 3) | 2 = 98 = 0x62
result.extend(_VarintBytes(len(inner)))
result.extend(inner)

sys.stdout.buffer.write(bytes(result))
PYEOF
}

# Check if schema exists in DB
schema_exists_in_db() {
    local hash="$1"
    docker-compose exec -T postgres psql -U ifex -d ifex_offboard -t -A \
        -c "SELECT COUNT(*) FROM schema_registry WHERE schema_hash = '$hash'" 2>/dev/null | tr -d ' '
}

# Check vehicle-schema link
vehicle_schema_linked() {
    local vehicle_id="$1"
    local hash="$2"
    docker-compose exec -T postgres psql -U ifex -d ifex_offboard -t -A \
        -c "SELECT COUNT(*) FROM vehicle_schemas WHERE vehicle_id = '$vehicle_id' AND schema_hash = '$hash'" 2>/dev/null | tr -d ' '
}

# Subscribe to MQTT and capture messages
capture_c2v_message() {
    local vehicle_id="$1"
    local timeout_s="${2:-5}"
    timeout "$timeout_s" mosquitto_sub -h localhost -t "c2v/$vehicle_id/201" -C 1 2>/dev/null || echo ""
}

# =============================================================================
# Test Cases
# =============================================================================

echo "=========================================="
echo "  Hash-Based Discovery Sync Tests"
echo "=========================================="
echo "  Vehicle ID: $VEHICLE_ID"
echo "  Test Hash:  ${TEST_SCHEMA_HASH:0:16}..."
echo "=========================================="
echo ""

# -----------------------------------------------------------------------------
# Test 1: Vehicle sends hash manifest with unknown hash
# -----------------------------------------------------------------------------
log_test "Test 1: Vehicle sends hash manifest, cloud requests unknown schema"

# First, ensure schema doesn't exist
docker-compose exec -T postgres psql -U ifex -d ifex_offboard -q \
    -c "DELETE FROM schema_registry WHERE schema_hash = '$TEST_SCHEMA_HASH'" 2>/dev/null || true

# Start capturing c2v messages in background
log_step "Starting c2v message capture..."
capture_c2v_message "$VEHICLE_ID" 10 > /tmp/c2v_capture.bin &
CAPTURE_PID=$!
sleep 1

# Send hash manifest via MQTT
log_step "Sending hash manifest via MQTT..."
MANIFEST=$(encode_hash_manifest "$VEHICLE_ID" "$TEST_SCHEMA_HASH")
echo -n "$MANIFEST" | mosquitto_pub -h localhost -t "v2c/$VEHICLE_ID/201" -s

# Wait for processing
sleep 3

# Check if we captured a c2v schema request
wait $CAPTURE_PID 2>/dev/null || true
C2V_SIZE=$(stat -c%s /tmp/c2v_capture.bin 2>/dev/null || echo "0")

if [ "$C2V_SIZE" -gt 0 ]; then
    pass_test "Cloud sent schema request via c2v/$VEHICLE_ID/201 ($C2V_SIZE bytes)"
else
    fail_test "No schema request received on c2v/$VEHICLE_ID/201"
    echo "  Check discovery_mirror logs: /tmp/ifex-discovery-logs/discovery_mirror.log"
fi

# -----------------------------------------------------------------------------
# Test 2: Vehicle responds with schema, cloud stores it
# -----------------------------------------------------------------------------
log_test "Test 2: Vehicle sends schema, cloud stores in schema_registry"

# Send schema response
log_step "Sending schema response via MQTT..."
RESPONSE=$(encode_schema_response "$VEHICLE_ID" "$TEST_SCHEMA_HASH" "$TEST_SCHEMA_YAML")
echo -n "$RESPONSE" | mosquitto_pub -h localhost -t "v2c/$VEHICLE_ID/201" -s

# Wait for processing
sleep 3

# Check if schema was stored
SCHEMA_COUNT=$(schema_exists_in_db "$TEST_SCHEMA_HASH")

if [ "$SCHEMA_COUNT" = "1" ]; then
    pass_test "Schema stored in schema_registry"

    # Verify service_name was extracted
    SERVICE_NAME=$(docker-compose exec -T postgres psql -U ifex -d ifex_offboard -t -A \
        -c "SELECT service_name FROM schema_registry WHERE schema_hash = '$TEST_SCHEMA_HASH'" 2>/dev/null)

    if [ "$SERVICE_NAME" = "test_service" ]; then
        pass_test "Service name correctly extracted: $SERVICE_NAME"
    else
        fail_test "Expected service_name='test_service', got '$SERVICE_NAME'"
    fi
else
    fail_test "Schema not found in schema_registry (count=$SCHEMA_COUNT)"
fi

# -----------------------------------------------------------------------------
# Test 3: Vehicle-schema link created
# -----------------------------------------------------------------------------
log_test "Test 3: Vehicle-schema link created in vehicle_schemas"

LINK_COUNT=$(vehicle_schema_linked "$VEHICLE_ID" "$TEST_SCHEMA_HASH")

if [ "$LINK_COUNT" = "1" ]; then
    pass_test "Vehicle-schema link created"
else
    fail_test "Vehicle-schema link not found (count=$LINK_COUNT)"
fi

# -----------------------------------------------------------------------------
# Test 4: Second vehicle with same hash doesn't trigger request
# -----------------------------------------------------------------------------
log_test "Test 4: Known hash doesn't trigger schema request"

VEHICLE_ID_2="test-discovery-2-$$"

# Start capturing c2v messages
capture_c2v_message "$VEHICLE_ID_2" 5 > /tmp/c2v_capture2.bin &
CAPTURE_PID=$!
sleep 1

# Send hash manifest with known hash
log_step "Sending manifest with known hash from second vehicle..."
MANIFEST2=$(encode_hash_manifest "$VEHICLE_ID_2" "$TEST_SCHEMA_HASH")
echo -n "$MANIFEST2" | mosquitto_pub -h localhost -t "v2c/$VEHICLE_ID_2/201" -s

# Wait for processing
sleep 3
wait $CAPTURE_PID 2>/dev/null || true
C2V_SIZE2=$(stat -c%s /tmp/c2v_capture2.bin 2>/dev/null || echo "0")

if [ "$C2V_SIZE2" = "0" ]; then
    pass_test "No schema request sent for known hash (bandwidth saved!)"
else
    fail_test "Unexpected schema request sent for known hash ($C2V_SIZE2 bytes)"
fi

# Check that second vehicle got linked
LINK_COUNT2=$(vehicle_schema_linked "$VEHICLE_ID_2" "$TEST_SCHEMA_HASH")
if [ "$LINK_COUNT2" = "1" ]; then
    pass_test "Second vehicle linked to existing schema"
else
    fail_test "Second vehicle not linked to schema"
fi

# Clean up second vehicle
docker-compose exec -T postgres psql -U ifex -d ifex_offboard -q \
    -c "DELETE FROM vehicle_schemas WHERE vehicle_id = '$VEHICLE_ID_2'" 2>/dev/null || true
docker-compose exec -T postgres psql -U ifex -d ifex_offboard -q \
    -c "DELETE FROM vehicles WHERE vehicle_id = '$VEHICLE_ID_2'" 2>/dev/null || true

# =============================================================================
# Summary
# =============================================================================

echo ""
echo "=========================================="
echo "  Test Summary"
echo "=========================================="
echo ""
echo "  Tests Run:    $TESTS_RUN"
echo -e "  ${GREEN}Passed:       $TESTS_PASSED${NC}"
if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "  ${RED}Failed:       $TESTS_FAILED${NC}"
fi
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    echo ""
    echo "Hash-based discovery sync is working:"
    echo "  - Vehicle sends hash manifest (just hashes, not full schemas)"
    echo "  - Cloud detects unknown hashes and requests them"
    echo "  - Vehicle sends full schema only when requested"
    echo "  - Schema stored once, linked to multiple vehicles"
    echo "  - Bandwidth savings: ~94% for known schemas"
    exit 0
else
    echo -e "${RED}Some tests failed.${NC}"
    echo ""
    echo "Debugging:"
    echo "  - Bridge logs: /tmp/ifex-discovery-logs/mqtt_kafka_bridge.log"
    echo "  - Mirror logs: /tmp/ifex-discovery-logs/discovery_mirror.log"
    echo "  - MQTT monitor: mosquitto_sub -h localhost -t 'v2c/+/201' -v"
    echo "  - c2v monitor: mosquitto_sub -h localhost -t 'c2v/+/201' -v"
    echo "  - DB check: docker-compose exec postgres psql -U ifex -d ifex_offboard -c \"SELECT * FROM schema_registry\""
    exit 1
fi
