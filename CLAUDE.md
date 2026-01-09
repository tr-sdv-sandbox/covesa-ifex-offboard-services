# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

IFEX Offboard Services is a cloud-side component for managing vehicle fleets using the COVESA IFEX standard. It provides:

1. **MQTT→Kafka Bridge** - Routes vehicle messages to Kafka topics by content_id, tracks vehicle online/offline status
2. **Discovery Mirror** - Syncs vehicle service registry to PostgreSQL with hash-based deduplication
3. **Scheduler Mirror** - Syncs scheduled job state to PostgreSQL
4. **Cloud APIs** - gRPC services for discovery, dispatcher, and scheduler
5. **Enrichment Exporter** - Exports vehicle metadata to Kafka for bridge consumption
6. **Fleet Dashboard** - Web UI for fleet management

## Architecture

```
Vehicle (MQTT) → mqtt_kafka_bridge → Kafka → {discovery,scheduler}_mirror → PostgreSQL
                      ↑                                                           ↓
              enrichment_exporter ←─────────────── vehicle_enrichment ←─── Cloud APIs
```

Content ID routing:
- 200 → ifex.rpc.200 (RPC request/response)
- 201 → ifex.discovery.201 (Service registry sync)
- 202 → ifex.scheduler.202 (Job state sync)

## Build Commands

```bash
# Install dependencies (Ubuntu 24.04)
./install_deps.sh

# Start infrastructure (Docker)
cd deploy && docker compose up -d

# Build
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j

# Build with tests
cmake .. -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON && make -j

# Run all tests
ctest --output-on-failure

# Run specific test suite
ctest -R scheduler_e2e --output-on-failure
ctest -L e2e --output-on-failure    # All E2E tests

# Run single test binary directly
./tests/scheduler_command_test
```

## Quick Start

```bash
# Full infrastructure startup (builds, populates DB, starts all services)
./deploy/start-infra.sh [num_vehicles]    # Default: 100000 vehicles
./deploy/stop-infra.sh                    # Stop all services

# Logs location
/tmp/ifex-logs/{mqtt_kafka_bridge,discovery_mirror,scheduler_mirror,...}.log
```

## Cloud API Services (gRPC)

| Service | Default Port | Purpose |
|---------|-------------|---------|
| `dispatcher_api` | 50100 | Execute RPCs on vehicle services |
| `discovery_api` | 50101 | Query fleet service registry |
| `scheduler_api` | 50102 | Create/manage scheduled jobs |

## Key Libraries

| Library | Purpose |
|---------|---------|
| `kafka_client` | KafkaProducer/KafkaConsumer wrappers for librdkafka |
| `postgres_client` | PostgresClient wrapper for libpq with result iteration |
| `envelope_codec` | Decode/encode protobuf sync envelopes |
| `mqtt_kafka_router` | MQTT subscription, content_id routing, vehicle context |
| `grpc_service_base` | Common gRPC server setup for cloud APIs |

## PostgreSQL Tables

| Table | Purpose |
|-------|---------|
| `vehicles` | Known vehicles with `is_online`, `last_seen_at` |
| `vehicle_enrichment` | Fleet assignment, region, model metadata |
| `schema_registry` | Deduplicated IFEX schemas by SHA-256 hash |
| `vehicle_schemas` | Vehicle-to-schema junction table |
| `jobs` | Scheduled jobs (from content_id=202) |
| `job_executions` | Job execution history |
| `offboard_calendar` | Cloud-side jobs pending sync to vehicles |
| `sync_state` | Per-vehicle sync sequence/checksum |

## Code Conventions

- **C++ Standard:** C++17
- **Namespace:** `ifex::offboard`
- **Logging:** glog (`LOG(INFO)`, `VLOG(1)`)
- **CLI flags:** gflags
- **Proto packages:** `swdv.discovery_sync_envelope`, `swdv.scheduler_sync_envelope`, `swdv.dispatcher_rpc_envelope`

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| MQTT_HOST | localhost | MQTT broker |
| KAFKA_BROKER | localhost:9092 | Kafka bootstrap server |
| POSTGRES_HOST | localhost | PostgreSQL host |
| POSTGRES_PORT | 5432 | PostgreSQL port |
| POSTGRES_DB | ifex_offboard | Database name |
| POSTGRES_USER | ifex | Database user |
| POSTGRES_PASSWORD | ifex_dev | Database password |

## Debugging

```bash
# View service logs
tail -f /tmp/ifex-logs/mqtt_kafka_bridge.log

# Query database
./deploy/query-db.sh "SELECT vehicle_id, is_online FROM vehicles WHERE is_online = true LIMIT 10"

# Monitor MQTT
mosquitto_sub -h localhost -t 'v2c/#' -v

# Kafka topics
docker exec ifex-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
```

## Offline Vehicle Command Delivery

Commands sent to offline vehicles are delivered when they reconnect. This relies on MQTT persistent sessions.

### How It Works

```
Cloud Service ──▶ MQTT Broker ──▶ Vehicle (offline)
                      │
                      ▼
               Queue (QoS 1)
                      │
               Vehicle reconnects
                      │
                      ▼
               Deliver queued messages
```

**Requirements:**
1. **Cloud publishes with QoS 1** - Ensures broker queues the message
2. **Vehicle uses `clean_session=false`** - Enables persistent subscriptions
3. **Vehicle subscribes before disconnect** - Broker knows to queue for this client

### MQTT Topic Patterns

| Direction | Pattern | QoS | Description |
|-----------|---------|-----|-------------|
| Cloud → Vehicle | `c2v/{vehicle_id}/{content_id}` | 1 | Commands to vehicle |
| Vehicle → Cloud | `v2c/{vehicle_id}/{content_id}` | 1 | Telemetry/sync from vehicle |

### Content IDs for c2v

| Content ID | Purpose | Payload |
|------------|---------|---------|
| 200 | RPC requests | `dispatcher_rpc_envelope` |
| 202 | Scheduler commands | `scheduler_sync_envelope` |

### Current Implementation Status

| Feature | Status | Notes |
|---------|--------|-------|
| MQTT QoS 1 publish | ✅ Implemented | Cloud services publish with QoS 1 |
| Vehicle persistent session | ✅ Implemented | Backend Transport uses `clean_session=false` |
| Broker message queueing | ✅ Works | Mosquitto queues for offline subscribers |
| Command delivery on reconnect | ✅ Tested | E2E test in `deploy/test-offline-vehicle.sh` |
| Delivery confirmation (c2v→v2c ack) | ❌ Not implemented | Cloud doesn't track if vehicle received command |
| Retry on delivery failure | ❌ Not implemented | No cloud-side retry mechanism |
| Command expiry/TTL | ❌ Not implemented | Stale commands may be delivered |

### Future Enhancements

1. **Delivery tracking** - Vehicle sends ack on `v2c/{vehicle_id}/ack`, cloud tracks pending commands
2. **Command TTL** - Include expiry timestamp, vehicle ignores stale commands
3. **Retry mechanism** - Cloud retries unacked commands with exponential backoff
4. **Offline queue visibility** - API to query pending commands for a vehicle

## Vehicle Online/Offline Status Tracking

Tracks vehicle connection status using MQTT Last Will and Testament (LWT) and heartbeat timeout.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              VEHICLE                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Backend Transport (mqtt_client.cpp)                                  │   │
│  │                                                                      │   │
│  │  Connect:                                                            │   │
│  │    1. mosquitto_will_set(topic, "0", retain=true)  ← LWT            │   │
│  │    2. mosquitto_connect()                                            │   │
│  │    3. mosquitto_publish(topic, "1", retain=true)   ← Online         │   │
│  │                                                                      │   │
│  │  topic = "v2c/{vehicle_id}/is_online"                               │   │
│  └──────────────────────────────┬──────────────────────────────────────┘   │
└─────────────────────────────────┼───────────────────────────────────────────┘
                                  │ MQTT (QoS 1, retained)
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           MOSQUITTO BROKER                                  │
│  - Stores retained message per topic                                        │
│  - On clean disconnect: does nothing                                        │
│  - On unexpected disconnect: publishes LWT ("0")                           │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │ Subscribed: v2c/#
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         mqtt_kafka_bridge                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ mqtt_kafka_router.cpp:on_message()                                   │   │
│  │                                                                      │   │
│  │  if topic.ends_with("/is_online"):                                  │   │
│  │    vehicle_id = extract_from_topic(topic)                           │   │
│  │    is_online = (payload == "1")                                     │   │
│  │    ┌──────────────────────────────────────────────────────────┐     │   │
│  │    │ produce(kafka_topic_status, vehicle_id, payload)         │─────┼───┼──▶ Kafka
│  │    └──────────────────────────────────────────────────────────┘     │   │
│  │    ┌──────────────────────────────────────────────────────────┐     │   │
│  │    │ status_callback_(vehicle_id, is_online)                  │     │   │
│  │    └─────────────────────────┬────────────────────────────────┘     │   │
│  └──────────────────────────────┼──────────────────────────────────────┘   │
│                                 │                                           │
│  ┌──────────────────────────────┼──────────────────────────────────────┐   │
│  │ main.cpp (status callback)   ▼                                       │   │
│  │                                                                      │   │
│  │  db->execute(                                                        │   │
│  │    "INSERT INTO vehicles (vehicle_id, is_online, last_seen_at)      │   │
│  │     VALUES ($1, $2, NOW())                                          │   │
│  │     ON CONFLICT (vehicle_id) DO UPDATE                              │   │
│  │     SET is_online = $2, last_seen_at = NOW()",                      │   │
│  │    {vehicle_id, is_online})                                         │   │
│  └──────────────────────────────┬──────────────────────────────────────┘   │
└─────────────────────────────────┼───────────────────────────────────────────┘
                                  │ SQL (libpq)
                                  ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            POSTGRESQL                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ vehicles                                                             │   │
│  │ ┌────────────────────┬───────────┬──────────────────────────────┐   │   │
│  │ │ vehicle_id         │ is_online │ last_seen_at                 │   │   │
│  │ ├────────────────────┼───────────┼──────────────────────────────┤   │   │
│  │ │ VIN00000000000001  │ true      │ 2026-01-05 08:30:00+00       │   │   │
│  │ │ VIN00000000000002  │ false     │ 2026-01-05 08:25:00+00       │   │   │
│  │ └────────────────────┴───────────┴──────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Code Path

| Step | File | Function/Code |
|------|------|---------------|
| 1. Set LWT | `mqtt_client.cpp:82-83` | `mosquitto_will_set(topic, "0", QoS=1, retain=true)` |
| 2. Publish online | `mqtt_client.cpp:291-292` | `mosquitto_publish(topic, "1", QoS=1, retain=true)` |
| 3. Receive MQTT | `mqtt_kafka_router.cpp:218` | `message_arrived()` → `on_message()` |
| 4. Detect suffix | `mqtt_kafka_router.cpp:383-386` | `topic.ends_with("/is_online")` |
| 5. Extract vehicle | `mqtt_kafka_router.cpp:389-391` | Parse `v2c/{vehicle_id}/is_online` |
| 6. Kafka produce | `mqtt_kafka_router.cpp:399-403` | `produce(status_kafka_topic_, ...)` |
| 7. DB callback | `mqtt_kafka_router.cpp:407-408` | `status_callback_(vehicle_id, is_online)` |
| 8. SQL upsert | `main.cpp:331-335` | `INSERT ... ON CONFLICT DO UPDATE` |

### Heartbeat Timeout

A background thread marks vehicles offline if no messages received:

```cpp
// main.cpp:347-372
while (g_running) {
    std::string timeout_interval = std::to_string(FLAGS_heartbeat_timeout_s) + " seconds";
    db->execute(
        "UPDATE vehicles SET is_online = false "
        "WHERE is_online = true AND last_seen_at < NOW() - INTERVAL '" + timeout_interval + "'");
    sleep(FLAGS_heartbeat_check_interval_s);
}
```

### Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--kafka_topic_status` | ifex.status | Kafka topic for status messages |
| `--heartbeat_timeout_s` | 60 | Seconds without message before marking offline |
| `--heartbeat_check_interval_s` | 10 | Interval between timeout checks |
| `--postgres_host` | localhost | PostgreSQL host for status updates |

### Testing

```bash
# E2E tests (requires infrastructure)
cd deploy && ./start-infra.sh
cd ../build && ./tests/status_e2e_test

# Query database
./deploy/query-db.sh "SELECT vehicle_id, is_online, last_seen_at FROM vehicles WHERE is_online = true LIMIT 10"
```

## Discovery Sync Protocol

Vehicles sync their service registry to cloud using a hash-based protocol (content_id=201).

```
VEHICLE                                         CLOUD
   │                                              │
   ├──── [hash1, hash2, hash3] ───────────────────▶  (just hashes)
   │                                              │
   │                               Unknown: hash3─┤
   │                                              │
   ◀──── [hash3] ──────────────────────────────────┤  (request)
   │                                              │
   ├──── {hash3: "yaml..."} ──────────────────────▶  (schema)
   │                                              │
```

**Bandwidth savings**: 100K vehicles × 5 services → ~10MB vs 5GB (old protocol)

### Database Schema (Normalized)

```sql
-- Unique schemas (deduplicated by hash)
service_schemas (
    schema_hash VARCHAR(64) PRIMARY KEY,
    ifex_schema TEXT,
    service_name, version,
    methods JSONB,              -- Pre-parsed
    struct_definitions JSONB,   -- Pre-parsed
    enum_definitions JSONB      -- Pre-parsed
)

-- Vehicle ↔ Schema links
vehicle_services (
    vehicle_id,
    schema_hash REFERENCES service_schemas
)
```

See `docs/fleet-dashboard-spec.md` for full implementation details.

## E2E Testing with Real Vehicles

The E2E tests run real vehicle containers to verify the complete cloud→vehicle→cloud round-trip.

### Prerequisites

**1. Build the vehicle Docker image (in covesa-ifex-core):**
```bash
cd ../covesa-ifex-core
./build-test-container.sh
```

This builds `ifex-vehicle:latest` containing:
- Discovery, Dispatcher, Scheduler, Backend Transport services
- Sync bridges (discovery, scheduler, dispatcher)
- Test services (echo, beverage, climate-comfort, defrost)

**IMPORTANT:** Rebuild the container whenever you change vehicle-side code!

**2. Start infrastructure:**
```bash
./deploy/start-infra.sh
```

### Running E2E Tests

```bash
# Build with tests
cmake -B build -DBUILD_TESTS=ON && cmake --build build

# Run all E2E tests
ctest --test-dir build -L e2e --output-on-failure

# Run specific suite
ctest --test-dir build -R dispatcher_e2e
ctest --test-dir build -R scheduler_e2e
ctest --test-dir build -R discovery_e2e
ctest --test-dir build -R status_e2e
```

### What the E2E Tests Verify

| Test Suite | Verifies |
|------------|----------|
| `dispatcher_e2e` | RPC round-trip: cloud API → Kafka → MQTT → vehicle echo service → response |
| `scheduler_e2e` | Job creation, trigger, execution on vehicle, result sync back |
| `discovery_e2e` | Service registry sync from vehicle to cloud |
| `status_e2e` | Vehicle online/offline status via MQTT LWT |

### Test Architecture

```
E2E Test Process
    │
    ├─ SetUpTestSuite()
    │   ├─ Verify Docker infra (postgres, kafka, mosquitto)
    │   ├─ Start mqtt_kafka_bridge
    │   ├─ Start discovery_mirror, scheduler_mirror
    │   ├─ Start ifex-vehicle Docker container
    │   ├─ Wait for vehicle online + echo_service registered
    │   └─ Start cloud API service being tested
    │
    ├─ Run tests (real vehicle responses)
    │
    └─ TearDownTestSuite()
        └─ Stop all services and clean up
```

## Fleet Dashboard

Web UI for fleet management at `http://localhost:8000`.

**Tabs:**
- **Fleet** - Vehicle list with online/offline status
- **Services** - Browse services discovered from vehicles
- **Calendar** - View/create scheduled jobs
- **Dispatch** - Execute RPC calls to vehicles

**Architecture:**
```
Browser → fleet_api.py → PostgreSQL
                      → MQTT (for RPC dispatch)
```

The dashboard uses pre-parsed JSONB from `service_schemas` to render dynamic forms based on IFEX method signatures.

See `docs/fleet-dashboard-spec.md` for UI specification.
