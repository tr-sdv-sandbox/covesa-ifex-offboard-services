# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

IFEX Offboard Services is a cloud-side component for managing vehicle fleets using the COVESA IFEX standard. It provides:

1. **MQTT→Kafka Bridge** - Routes vehicle messages to Kafka topics by content_id
2. **Discovery Mirror** - Syncs vehicle service registry to PostgreSQL
3. **Scheduler Mirror** - Syncs scheduled job state to PostgreSQL
4. **RPC Gateway** - Enables cloud→vehicle method invocation

## Architecture

```
MQTT (v2c/#) → mqtt_kafka_bridge → Kafka → {discovery,scheduler,rpc}_mirror → PostgreSQL
```

Content ID routing:
- 200 → ifex.rpc.200 (RPC request/response)
- 201 → ifex.discovery.201 (Service registry sync)
- 202 → ifex.scheduler.202 (Job state sync)

## Build Commands

```bash
# Start infrastructure (Docker)
cd docker
docker compose up -d

# Build services
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j

# Run individual service
./mqtt_kafka_bridge --mqtt_host=localhost --kafka_broker=localhost:9092
./discovery_mirror --kafka_broker=localhost:9092 --postgres_host=localhost
./scheduler_mirror --kafka_broker=localhost:9092 --postgres_host=localhost
./rpc_gateway --kafka_broker=localhost:9092 --postgres_host=localhost
```

## Directory Structure

```
covesa-ifex-offboard-services/
├── docker/
│   ├── docker-compose.yml    # Mosquitto, Kafka, PostgreSQL
│   └── mosquitto.conf
├── sql/
│   └── schema.sql            # PostgreSQL schema
├── proto/                    # Copied from covesa-ifex-core
│   ├── dispatcher-rpc-envelope.proto
│   ├── discovery-sync-envelope.proto
│   └── scheduler-sync-envelope.proto
├── libs/
│   ├── kafka_client/         # librdkafka wrapper
│   ├── postgres_client/      # libpq wrapper
│   └── envelope_codec/       # Protobuf codecs
└── services/
    ├── mqtt_kafka_bridge/    # MQTT → Kafka router
    ├── discovery_mirror/     # Service registry to PostgreSQL
    ├── scheduler_mirror/     # Job state to PostgreSQL
    └── rpc_gateway/          # RPC request/response tracking
```

## Key Libraries

| Library | Purpose |
|---------|---------|
| `kafka_client` | KafkaProducer/KafkaConsumer wrappers for librdkafka |
| `postgres_client` | PostgresClient wrapper for libpq with result iteration |
| `envelope_codec` | Decode/encode protobuf sync envelopes |

## PostgreSQL Tables

| Table | Purpose |
|-------|---------|
| `vehicles` | Known vehicles with first/last seen timestamps |
| `services` | Service registry (from content_id=201) |
| `jobs` | Scheduled jobs (from content_id=202) |
| `job_executions` | Job execution history |
| `rpc_requests` | Cloud→vehicle RPC tracking |
| `sync_state` | Per-vehicle sync sequence/checksum |

## Dependencies

**System packages:**
- librdkafka-dev (Kafka C++ client)
- libpq-dev (PostgreSQL C client)
- libpaho-mqttcpp-dev (MQTT C++ client)
- protobuf-compiler, libprotobuf-dev
- libglog-dev, libgflags-dev

**Docker images:**
- eclipse-mosquitto:2
- confluentinc/cp-kafka:7.5.0
- confluentinc/cp-zookeeper:7.5.0
- postgres:16

## Code Conventions

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
