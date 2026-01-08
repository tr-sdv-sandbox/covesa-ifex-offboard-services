# IFEX Offboard Services Specification

## Overview

The IFEX Offboard Services provide cloud-side infrastructure for managing vehicle fleets using the COVESA IFEX standard. The system ingests telemetry from vehicles, stores state in PostgreSQL, and exposes APIs for fleet management.

**Core principle**: PostgreSQL is the source of truth. Kafka is the message bus for decoupling and enrichment distribution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              VEHICLES                                    │
│         Backend Transport publishes to MQTT (v2c/{vehicle_id}/*)        │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │ MQTT (QoS 1, retained)
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         MOSQUITTO BROKER                                 │
│    v2c/#  (vehicle-to-cloud)     c2v/#  (cloud-to-vehicle)             │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       mqtt_kafka_bridge                                  │
│                                                                          │
│  Routes by content_id:              Enriches with VehicleContext:       │
│    200 → ifex.rpc.200                 fleet_id, region, model           │
│    201 → ifex.discovery.201                                              │
│    202 → ifex.scheduler.202         Transforms:                          │
│    is_online → ifex.status            vehicle_proto → offboard_proto    │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                            KAFKA                                         │
│                                                                          │
│  ifex.discovery.201      ifex.scheduler.202      ifex.rpc.200           │
│  ifex.status             ifex.vehicle.enrichment                         │
│  ifex.c2v.rpc            ifex.c2v.scheduler                             │
└───────┬─────────────────────────┬─────────────────────────┬─────────────┘
        │                         │                         │
        ▼                         ▼                         ▼
┌───────────────┐       ┌─────────────────┐       ┌─────────────────┐
│discovery_mirror│       │scheduler_mirror │       │   rpc_gateway   │
└───────┬───────┘       └────────┬────────┘       └────────┬────────┘
        │                        │                         │
        └────────────────────────┼─────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          POSTGRESQL                                      │
│                                                                          │
│  vehicles              vehicle_enrichment (source of truth)             │
│  schema_registry       vehicle_schemas                                   │
│  jobs                  job_executions                                    │
│  rpc_requests          sync_state                                        │
└─────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      enrichment_exporter                                 │
│           Polls vehicle_enrichment → Kafka ifex.vehicle.enrichment      │
└─────────────────────────────────────────────────────────────────────────┘
```

## Functional Specifications

The protocol-level behavior is defined in separate specifications:

| Protocol | Content ID | Specification |
|----------|------------|---------------|
| Discovery Sync | 201 | [discovery-sync-protocol.md](../../covesa-ifex-core/docs/discovery-sync-protocol.md) |
| Scheduler Sync | 202 | [scheduler-sync-protocol.md](../../covesa-ifex-core/docs/scheduler-sync-protocol.md) |
| RPC Dispatch | 200 | [rpc-protocol.md](../../covesa-ifex-core/docs/rpc-protocol.md) |

## Content ID Routing

| Content ID | Direction | Purpose | Kafka Topic |
|------------|-----------|---------|-------------|
| 200 | v2c | RPC responses | `ifex.rpc.200` |
| 200 | c2v | RPC requests | `ifex.c2v.rpc` |
| 201 | v2c | Discovery sync | `ifex.discovery.201` |
| 202 | v2c | Scheduler sync | `ifex.scheduler.202` |
| 202 | c2v | Scheduler commands | `ifex.c2v.scheduler` |
| special | v2c | Online status | `ifex.status` |

## Components

### Ingestion Layer

#### mqtt_kafka_bridge

Entry point for all vehicle telemetry. Performs:

1. **Topic parsing**: Extracts `vehicle_id` and `content_id` from MQTT topic
2. **Protocol decoding**: Decodes vehicle-side protobuf
3. **Enrichment lookup**: Attaches `VehicleContext` (fleet_id, region, model)
4. **Offboard wrapping**: Creates offboard envelope with metadata
5. **Kafka production**: Routes to appropriate topic by content_id

**Vehicle Status Tracking**:
- Detects `v2c/{vehicle_id}/is_online` messages (MQTT LWT)
- Updates `vehicles.is_online` in PostgreSQL
- Heartbeat timeout marks stale vehicles offline (60s default)

**Enrichment Consumer**:
- Loads `ifex.vehicle.enrichment` topic on startup
- Maintains in-memory `VehicleContextStore`
- Background thread consumes updates

#### discovery_mirror

Consumes `ifex.discovery.201`, implements hash-based sync protocol:

1. Receives hash manifest from vehicle
2. Checks `schema_registry` for unknown hashes
3. Sends schema request via c2v if needed
4. Stores schemas with pre-parsed JSONB
5. Links vehicles to schemas in `vehicle_schemas`

#### scheduler_mirror

Consumes `ifex.scheduler.202`:

1. Processes job events (CREATED, UPDATED, DELETED, EXECUTED)
2. Stores jobs in `jobs` table
3. Records executions in `job_executions`
4. Tracks sync state (sequence, checksum)

#### rpc_gateway

Consumes `ifex.rpc.200`:

1. Matches responses to pending requests by `correlation_id`
2. Updates `rpc_requests` with response data
3. Detects timeouts for unresponded requests

### Exporter Layer

#### enrichment_exporter

Syncs PostgreSQL → Kafka:

1. Full export on startup
2. Polls `vehicle_enrichment` for changes (by `updated_at`)
3. Publishes to compacted topic `ifex.vehicle.enrichment`
4. Key = vehicle_id (consumers get latest state)

### API Layer

#### fleet_api (Flask)

REST API for Fleet Dashboard:

| Endpoint | Purpose |
|----------|---------|
| `GET /api/vehicles` | List vehicles with enrichment |
| `GET /api/schemas` | List unique service schemas |
| `GET /api/schemas/<name>` | Get schema with methods/types |
| `POST /api/rpc` | Dispatch RPC to vehicle |
| `POST /api/calendar/<vehicle_id>` | Schedule job |
| `GET /api/calendar/<vehicle_id>` | Get vehicle calendar |

## Transformations

### Vehicle → Offboard Envelope

```
vehicle_proto (from vehicle)
    │
    ├─ metadata {
    │      vehicle_id      (from MQTT topic, ACL-verified)
    │      fleet_id        (from enrichment)
    │      region          (from enrichment)
    │      bridge_id       (this bridge instance)
    │      received_at_ns  (when MQTT received)
    │      processed_at_ns (when Kafka produced)
    │  }
    │
    └─ payload: original vehicle_proto
```

### Schema YAML → Pre-parsed JSONB

When storing schemas in `schema_registry`:

```yaml
# Input: IFEX YAML
name: climate_service
version: 1.0.0
namespaces:
  - name: climate
    methods:
      - name: set_temperature
        input:
          - name: zone
            datatype: string
          - name: temperature
            datatype: float
```

```json
// Output: Pre-parsed JSONB
{
  "methods": [
    {
      "namespace": "climate",
      "name": "set_temperature",
      "input": [
        {"name": "zone", "datatype": "string"},
        {"name": "temperature", "datatype": "float"}
      ]
    }
  ],
  "struct_definitions": {},
  "enum_definitions": {}
}
```

## Database Schema

### Core Tables

| Table | Purpose |
|-------|---------|
| `vehicles` | Vehicle records with online status |
| `vehicle_enrichment` | Metadata (source of truth → Kafka) |
| `schema_registry` | Unique schemas by SHA-256 hash |
| `vehicle_schemas` | Vehicle ↔ schema junction |
| `jobs` | Scheduled jobs from vehicles |
| `job_executions` | Job execution history |
| `rpc_requests` | RPC request/response tracking |
| `sync_state` | Per-vehicle sequence/checksum |

### Key Relationships

```
vehicles (1) ──── (1) vehicle_enrichment
    │
    └── (1) ──── (*) vehicle_schemas ──── (*) ──── (1) schema_registry
    │
    └── (1) ──── (*) jobs ──── (1) ──── (*) job_executions
    │
    └── (1) ──── (*) rpc_requests
```

## Enrichment Data Flow

PostgreSQL is the source of truth for enrichment:

```
Admin UI / Import Script
    │
    ▼
PostgreSQL: vehicle_enrichment
    │
    │ (polled by enrichment_exporter)
    ▼
Kafka: ifex.vehicle.enrichment (compacted)
    │
    │ (consumed by mqtt_kafka_bridge)
    ▼
VehicleContextStore (in-memory)
    │
    │ (used during transformation)
    ▼
Offboard envelope metadata
```

## Offline Vehicle Support

Commands sent to offline vehicles are delivered when they reconnect:

1. Cloud publishes with **QoS 1** → broker queues
2. Vehicle uses **persistent sessions** (`clean_session=false`)
3. Vehicle subscribes to `c2v/{vehicle_id}/#` before disconnect
4. On reconnect → broker delivers queued messages

**Current limitations**:
- No delivery confirmation (vehicle doesn't ACK)
- No retry mechanism
- No command expiry/TTL

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MQTT_HOST` | localhost | MQTT broker |
| `KAFKA_BROKER` | localhost:9092 | Kafka bootstrap |
| `POSTGRES_HOST` | localhost | PostgreSQL host |
| `POSTGRES_DB` | ifex_offboard | Database name |
| `HEARTBEAT_TIMEOUT_S` | 60 | Seconds before marking offline |

### Kafka Topics

| Topic | Type | Key | Purpose |
|-------|------|-----|---------|
| `ifex.discovery.201` | Standard | vehicle_id | Discovery sync |
| `ifex.scheduler.202` | Standard | vehicle_id | Scheduler sync |
| `ifex.rpc.200` | Standard | correlation_id | RPC responses |
| `ifex.status` | Compacted | vehicle_id | Vehicle status |
| `ifex.vehicle.enrichment` | Compacted | vehicle_id | Enrichment data |
| `ifex.c2v.rpc` | Standard | vehicle_id | RPC requests |
| `ifex.c2v.scheduler` | Standard | vehicle_id | Scheduler commands |

## Scalability Considerations

### Hash-Based Discovery

With 100K vehicles × 5 services:
- **Without hashing**: 100K × 5 × ~1KB = ~500MB daily
- **With hashing**: 100K × 5 × 64B = ~32MB daily (hashes only)
- **Savings**: ~94% bandwidth reduction for steady-state

### Kafka Partitioning

- Key by `vehicle_id` for per-vehicle ordering
- Partition count = expected consumer parallelism
- Compacted topics for status/enrichment (latest state only)

### PostgreSQL Indexes

Critical indexes for query performance:
- `schema_registry(service_name)` - schema lookups
- `vehicle_schemas(vehicle_id)` - per-vehicle services
- `jobs(vehicle_id, status)` - calendar queries
- `rpc_requests(correlation_id)` - response matching
