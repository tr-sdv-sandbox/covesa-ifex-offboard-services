# Offboard Platform Refactor Plan

## Goal

Create a transport abstraction layer in ifex-offboard-services that mirrors the `cloud_backend_transport_service` IFEX API from ifex-core, but backed by Kafka + MQTT for production use.

## Current State

```
ifex-core (testing):
  cloud/cloud-backend-transport/
    └── CloudBackendTransportClient/Server  # Simple MQTT-only implementation

ifex-offboard-services (production):
  services/ingestion/
    ├── mqtt_kafka_bridge/      # MQTT → Kafka routing
    ├── discovery_mirror/       # Kafka → PostgreSQL for discovery
    └── scheduler_mirror/       # Kafka → PostgreSQL for scheduler

  services/api/
    ├── discovery_api/          # gRPC API for discovery queries
    ├── dispatcher_api/         # gRPC API for RPC dispatch
    └── scheduler_api/          # gRPC API for scheduler + job commands
```

**Problem:** Services directly use Kafka/PostgreSQL. No abstraction for:
- Swapping transport for testing (use simple ifex-core transport)
- Independent transport testing
- Consistent API between ifex-core and offboard

## Target Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Cloud Services                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                         │
│  │ discovery_  │  │ dispatcher_ │  │ scheduler_  │                         │
│  │ service     │  │ service     │  │ service     │                         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                         │
│         │                │                │                                 │
│         └────────────────┼────────────────┘                                 │
│                          │                                                  │
│                          ▼                                                  │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │              cloud_backend_transport_service API                       │ │
│  │  (IFEX-defined interface: send_to_vehicle, on_vehicle_message, etc.) │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                          │                                                  │
│         ┌────────────────┼────────────────┐                                │
│         │                │                │                                 │
│         ▼                ▼                ▼                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                         │
│  │ Simple      │  │ KafkaMqtt   │  │ Mock        │                         │
│  │ Transport   │  │ Transport   │  │ Transport   │                         │
│  │ (ifex-core) │  │ (production)│  │ (unit tests)│                         │
│  └─────────────┘  └─────────────┘  └─────────────┘                         │
│         │                │                                                  │
│         ▼                ▼                                                  │
│      MQTT only     Kafka + MQTT                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
ifex-offboard-services/
├── libs/
│   └── offboard-platform/
│       ├── CMakeLists.txt
│       ├── ifex/
│       │   └── cloud-backend-transport-service.yml  # Copied from ifex-core
│       ├── include/
│       │   ├── offboard_transport.hpp         # Abstract interface (matches IFEX)
│       │   └── kafka_mqtt_transport.hpp       # Kafka+MQTT implementation
│       ├── src/
│       │   └── kafka_mqtt_transport.cpp
│       └── tests/
│           ├── kafka_mqtt_transport_test.cpp  # Unit tests
│           └── transport_integration_test.cpp # Kafka+MQTT integration
│
├── services/
│   ├── cloud/
│   │   ├── discovery_service.hpp/cpp    # Uses OffboardTransport interface
│   │   ├── dispatcher_service.hpp/cpp
│   │   └── scheduler_service.hpp/cpp
│   └── ingestion/
│       └── mqtt_kafka_bridge/           # Remains: MQTT→Kafka routing layer
│
└── tests/
    ├── unit/                            # Existing unit tests
    ├── integration/
    │   └── transport_integration_test.cpp
    └── e2e/
        ├── services_simple_transport_test.cpp   # Services + simple transport
        └── services_kafka_mqtt_test.cpp         # Services + Kafka+MQTT
```

## Implementation Steps

### Phase 1: Create offboard-platform Library

**1.1 Copy IFEX spec and generate proto**
```bash
mkdir -p libs/offboard-platform/ifex
cp <ifex-core>/cloud/ifex/cloud-backend-transport-service.yml libs/offboard-platform/ifex/
# Generate proto (add to generate script)
```

**1.2 Define C++ interface (offboard_transport.hpp)**

```cpp
namespace ifex::offboard {

// Matches IFEX cloud_backend_transport_service API
class OffboardTransport {
public:
    virtual ~OffboardTransport() = default;

    // === Methods (from IFEX) ===
    virtual SendResponse SendToVehicle(const SendRequest& request) = 0;
    virtual VehicleStatus GetVehicleStatus(const std::string& vehicle_id) = 0;
    virtual ChannelInfo GetChannelInfo() = 0;
    virtual QueueStatus GetQueueStatus(const std::string& vehicle_id) = 0;
    virtual TransportStats GetStats() = 0;
    virtual bool IsHealthy() = 0;

    // === Event callbacks (from IFEX) ===
    using VehicleMessageCallback = std::function<void(const VehicleMessage&)>;
    using AckCallback = std::function<void(const DeliveryAck&)>;
    using StatusCallback = std::function<void(const VehicleStatusEvent&)>;
    using QueueCallback = std::function<void(const QueueStatus&)>;

    virtual void SetOnVehicleMessage(VehicleMessageCallback cb) = 0;
    virtual void SetOnAck(AckCallback cb) = 0;
    virtual void SetOnVehicleStatus(StatusCallback cb) = 0;
    virtual void SetOnQueueStatusChanged(QueueCallback cb) = 0;

    // === Lifecycle ===
    virtual bool Start() = 0;
    virtual void Stop() = 0;
};

}  // namespace ifex::offboard
```

**1.3 Implement KafkaMqttTransport**

- Subscribe to Kafka topics for inbound messages (from mqtt_kafka_bridge)
- Publish to MQTT for outbound messages (c2v)
- Track vehicle status from `is_online` topic
- Maintain per-vehicle sequence numbers
- Implement queue management with persistence levels

### Phase 2: Refactor mqtt_kafka_bridge

Keep mqtt_kafka_bridge as the **ingestion layer**:
- MQTT subscription (v2c/# topics)
- Route to Kafka by content_id
- Track vehicle online/offline status
- Update PostgreSQL vehicles table

The KafkaMqttTransport:
- Consumes FROM Kafka (for v2c messages from vehicles)
- Produces TO Kafka (for c2v messages to vehicles)
- mqtt_kafka_bridge handles both directions of MQTT↔Kafka bridging

```
v2c: Vehicle ──MQTT──▶ mqtt_kafka_bridge ──Kafka──▶ KafkaMqttTransport ──▶ Service
c2v: Vehicle ◀──MQTT── mqtt_kafka_bridge ◀──Kafka── KafkaMqttTransport ◀── Service
```

**Design decision:** For c2v, KafkaMqttTransport publishes to Kafka, and mqtt_kafka_bridge subscribes and forwards to MQTT. This keeps all messages flowing through Kafka for consistency, replay-ability, and unified monitoring.

### Phase 3: Create Cloud Services Using Abstraction

Refactor services to depend on `OffboardTransport` interface:

```cpp
class SchedulerService {
public:
    explicit SchedulerService(std::shared_ptr<OffboardTransport> transport,
                              std::shared_ptr<PostgresClient> db);

    void Start() {
        transport_->SetOnVehicleMessage([this](const VehicleMessage& msg) {
            HandleVehicleSync(msg);
        });
        transport_->Start();
    }

private:
    void HandleVehicleSync(const VehicleMessage& msg);
    void SendCommand(const std::string& vehicle_id, const Command& cmd);

    std::shared_ptr<OffboardTransport> transport_;
    std::shared_ptr<PostgresClient> db_;
};
```

### Phase 4: Layered Integration Tests

**4.1 Transport-only test (Kafka+MQTT layer)**
```cpp
TEST(KafkaMqttTransportTest, SendReceiveRoundTrip) {
    // Start Kafka, MQTT, mqtt_kafka_bridge
    // Create KafkaMqttTransport
    // Send message to vehicle
    // Verify arrives via MQTT
    // Simulate vehicle response
    // Verify callback fires
}
```

**4.2 Services + simple transport test (fast CI)**
```cpp
TEST(SchedulerServiceTest, CreateJobWithSimpleTransport) {
    // Use ifex-core's simple CloudBackendTransport
    auto transport = std::make_shared<SimpleTransport>();
    auto service = SchedulerService(transport, mock_db);

    // Create job via gRPC
    // Verify command sent via transport
    // Simulate vehicle ack
    // Verify job state updated
}
```

**4.3 Full E2E test (services + Kafka+MQTT)**
```cpp
TEST(SchedulerE2ETest, CreateJobFullStack) {
    // Start full infrastructure
    // Start KafkaMqttTransport
    // Start SchedulerService
    // Start real vehicle container
    // Create job → verify vehicle receives → verify execution syncs back
}
```

## Migration Path

1. **Create offboard-platform library** (no breaking changes)
2. **Add KafkaMqttTransport** (parallel to existing code)
3. **Create new service implementations** using abstraction
4. **Add integration tests** for new services
5. **Deprecate old direct-Kafka services** once new ones are proven
6. **Remove deprecated code**

## Dependencies

| From | To |
|------|-----|
| offboard-platform | kafka_client, mqtt (mosquitto), proto generated from IFEX |
| cloud services | offboard-platform, postgres_client, grpc |
| integration tests | Docker (Kafka, MQTT, PostgreSQL) |

## Design Decisions

1. **Kafka topic structure for c2v:** KafkaMqttTransport publishes to Kafka, mqtt_kafka_bridge forwards to MQTT.
   - All messages flow through Kafka for consistency and replay-ability

2. **Shared vs separate transport instances:** One transport per content_id.
   - Matches IFEX spec: "bound to ONE content_id"

3. **PostgreSQL access:** Transport layer handles vehicle status DB updates, service layer handles business data.
   - Status is a transport-level concern
