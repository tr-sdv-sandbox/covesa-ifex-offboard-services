# COVESA IFEX Offboard Services

Cloud-side services for managing vehicle fleets using the COVESA IFEX standard.

## Overview

This component provides offboard (cloud) infrastructure for:
- **Service Discovery Mirroring** - Track which services are available on each vehicle
- **Scheduler State Mirroring** - Track scheduled jobs and execution results
- **RPC Gateway** - Execute methods on vehicle services from the cloud

## Architecture

```
Vehicle(s)                           Cloud (Docker Compose)
    |                                     |
    | MQTT (v2c/{vin}/{content_id})      |
    v                                     v
┌────────────┐                    ┌──────────────────┐
│ mock_bemp  │ ─────────────────► │  MQTT Broker     │
└────────────┘                    │  (Mosquitto)     │
                                  └────────┬─────────┘
                                           │
                                  ┌────────▼─────────┐
                                  │ mqtt-kafka-bridge│
                                  └────────┬─────────┘
                                           │
                                  ┌────────▼─────────┐
                                  │     Kafka        │
                                  │ - ifex.rpc.200   │
                                  │ - ifex.discovery.201
                                  │ - ifex.scheduler.202
                                  └────────┬─────────┘
                                           │
                    ┌──────────────────────┼──────────────────────┐
                    │                      │                      │
           ┌────────▼────────┐   ┌────────▼────────┐   ┌────────▼────────┐
           │ RPC Gateway     │   │ Discovery Mirror│   │ Scheduler Mirror│
           └────────┬────────┘   └────────┬────────┘   └────────┬────────┘
                    │                      │                      │
                    └──────────────────────┼──────────────────────┘
                                           │
                                  ┌────────▼─────────┐
                                  │   PostgreSQL     │
                                  └──────────────────┘
```

## Quick Start

### 1. Start Infrastructure

```bash
cd docker
docker compose up -d

# Verify services are running
docker compose ps

# Optional: Start Kafka UI for debugging
docker compose --profile debug up -d
# Access at http://localhost:8080
```

### 2. Build Services

```bash
# Install dependencies (Ubuntu/Debian)
sudo apt install -y \
    librdkafka-dev \
    libpq-dev \
    libpaho-mqttcpp-dev \
    libprotobuf-dev \
    protobuf-compiler \
    libglog-dev \
    libgflags-dev

# Build
mkdir build && cd build
cmake ..
make -j
```

### 3. Run Services

```bash
# Terminal 1: MQTT→Kafka bridge
./mqtt_kafka_bridge

# Terminal 2: Discovery mirror
./discovery_mirror

# Terminal 3: Scheduler mirror
./scheduler_mirror

# Terminal 4: RPC gateway
./rpc_gateway
```

## Services

### mqtt_kafka_bridge

Routes MQTT messages to Kafka topics based on content_id:

| MQTT Topic | Kafka Topic | Content |
|------------|-------------|---------|
| v2c/{vin}/200 | ifex.rpc.200 | RPC responses |
| v2c/{vin}/201 | ifex.discovery.201 | Service registry sync |
| v2c/{vin}/202 | ifex.scheduler.202 | Scheduler state sync |

### discovery_mirror

Consumes `ifex.discovery.201` and maintains service registry in PostgreSQL:
- Tracks which services are registered on each vehicle
- Updates service status (AVAILABLE, UNAVAILABLE, etc.)
- Records heartbeats and connection state

### scheduler_mirror

Consumes `ifex.scheduler.202` and maintains job state in PostgreSQL:
- Tracks scheduled jobs and their parameters
- Records job execution results
- Maintains next_run_time for recurring jobs

### rpc_gateway

Consumes `ifex.rpc.200` and tracks RPC request/response lifecycle:
- Records pending requests
- Matches responses to requests by correlation_id
- Marks timed-out requests

## PostgreSQL Schema

```sql
-- Key tables
vehicles          -- Known vehicles
services          -- Service registry per vehicle
jobs              -- Scheduled jobs per vehicle
job_executions    -- Job execution history
rpc_requests      -- RPC request/response tracking
sync_state        -- Per-vehicle sync sequence tracking
```

See `sql/schema.sql` for full schema.

## Configuration

All services accept command-line flags:

```bash
./discovery_mirror \
    --kafka_broker=kafka:9092 \
    --kafka_group=discovery-mirror \
    --postgres_host=postgres \
    --postgres_db=ifex_offboard
```

Environment variables are also supported (uppercase, underscores):
- `KAFKA_BROKER`, `POSTGRES_HOST`, etc.

## Content ID Assignments

| ID | Direction | Purpose |
|----|-----------|---------|
| 200 | Bidirectional | Dispatcher RPC forwarding |
| 201 | Vehicle→Cloud | Discovery state sync |
| 202 | Vehicle→Cloud | Scheduler state sync |

## Development

### Adding a New Mirror Service

1. Create proto codec in `libs/envelope_codec/`
2. Create store class for PostgreSQL persistence
3. Create main.cpp with Kafka consumer loop
4. Add to CMakeLists.txt

### Testing with Mock Data

```bash
# Publish test message to MQTT
mosquitto_pub -t "v2c/vehicle-001/201" -f test_discovery_sync.bin

# Check Kafka topic
kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic ifex.discovery.201 --from-beginning

# Query PostgreSQL
psql -h localhost -U ifex ifex_offboard -c "SELECT * FROM services"
```

## Dependencies

- librdkafka >= 2.0
- libpq >= 14
- paho.mqtt.cpp >= 1.3
- protobuf >= 3.21
- glog, gflags

## License

Apache 2.0
