# IFEX Fleet UI Architecture

Multi-faceted dashboard for fleet management with service discovery, scheduling, and immediate dispatch.

## Overview

The UI is organized into three main facets:

1. **Service Explorer** - Service discovery + dynamic form generation
2. **Calendar View** - Fleet → Vehicle → Calendar for scheduling
3. **Dispatch Now** - Immediate command execution

All facets share:
- Service discovery API for available services/methods
- Dynamic form generation from IFEX schema
- Vehicle selection/context

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Fleet Dashboard                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Fleet     │  │  Service    │  │  Calendar   │  │  Dispatch   │        │
│  │  Overview   │  │  Explorer   │  │    View     │  │    Now      │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
│         │                │                │                │                │
│         └────────────────┼────────────────┼────────────────┘                │
│                          ▼                ▼                                 │
│              ┌─────────────────────────────────────┐                        │
│              │     Shared Components               │                        │
│              │  - Vehicle Selector                 │                        │
│              │  - Service/Method Picker            │                        │
│              │  - Dynamic Form Builder             │                        │
│              │  - Parameter Input                  │                        │
│              └─────────────────────────────────────┘                        │
│                          │                                                  │
└──────────────────────────┼──────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            Fleet API (Python/Flask)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│  /api/services              - Service discovery (IFEX schemas)              │
│  /api/services/{name}       - Service details with struct/enum defs         │
│  /api/vehicles              - Vehicle list with filters                     │
│  /api/vehicles/{id}         - Vehicle details (services, jobs)              │
│  /api/vehicles/{id}/services - Services for specific vehicle                │
│  /api/rpc                   - Immediate dispatch (POST)                     │
│  /api/schedule              - Schedule job (POST)                           │
│  /api/jobs                  - Query scheduled jobs                          │
│  /api/calendar/{vehicle}    - Calendar events for vehicle                   │
└─────────────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PostgreSQL                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  vehicles              - Known vehicles (is_online, last_seen)              │
│  vehicle_enrichment    - Fleet/region/model metadata                        │
│  services              - Per-vehicle service registry (from discovery)      │
│  service_schemas       - NEW: Cached IFEX schemas for form generation       │
│  jobs                  - Scheduled jobs (synced from vehicle)               │
│  offboard_calendar     - NEW: Cloud-side calendar (pre-sync)                │
│  job_executions        - Execution history                                  │
│  rpc_requests          - RPC tracking                                       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Facet 1: Service Explorer

**Purpose:** Discover available services and execute methods with dynamically generated forms.

**Reference:** `ifex-core/test-gui/ifex_explorer_gui.html`

### UI Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Service Explorer                                              [Vehicle ▼]  │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────┐  ┌────────────────────────────────────────────┐  │
│  │ Services             │  │ climate_comfort_service                    │  │
│  ├──────────────────────┤  │                                            │  │
│  │ ▶ climate_comfort    │  │ Description: Cabin comfort control          │  │
│  │   beverage_service   │  │ Version: 1.0.0                              │  │
│  │   defrost_service    │  │ Address: localhost:50062                    │  │
│  │   echo_service       │  │                                            │  │
│  │                      │  │ Methods:                                   │  │
│  │                      │  │  ├─ set_comfort_level (schedulable)        │  │
│  │                      │  │  ├─ get_current_state                      │  │
│  │                      │  │  └─ configure_zones                        │  │
│  │                      │  │                                            │  │
│  └──────────────────────┘  │ ────────────────────────────────────────── │  │
│                            │                                            │  │
│                            │ set_comfort_level                          │  │
│                            │                                            │  │
│                            │ Parameters:                                │  │
│                            │ ┌────────────────────────────────────────┐ │  │
│                            │ │ comfort_settings (struct)              │ │  │
│                            │ │  ├─ target_level: [▼ COZY      ]       │ │  │
│                            │ │  ├─ zones: ☑ DRIVER ☑ PASSENGER ☐ REAR │ │  │
│                            │ │  └─ options:                           │ │  │
│                            │ │      ├─ max_temp: [25.0]               │ │  │
│                            │ │      └─ eco_mode: ☐                    │ │  │
│                            │ └────────────────────────────────────────┘ │  │
│                            │                                            │  │
│                            │ [Form View] [JSON View]                    │  │
│                            │                                            │  │
│                            │ [Execute Now] [Schedule...]                │  │
│                            └────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. Service List (Sidebar)
- Fetch from `/api/services` or `/api/vehicles/{id}/services`
- Show schedulable badge for methods with `x_scheduling.enabled`
- Click to expand and show methods

#### 2. Dynamic Form Builder
Generates form fields from IFEX parameter types:

| IFEX Type | Form Element |
|-----------|--------------|
| `string` | `<input type="text">` |
| `uint32`, `int32` | `<input type="number">` |
| `float`, `double` | `<input type="number" step="0.1">` |
| `boolean` | `<input type="checkbox">` |
| Enum | `<select>` with options |
| Enum[] (array) | Checkbox group |
| Struct | Nested fieldset with border |

#### 3. Form/JSON Toggle
- Form view: Generated form fields
- JSON view: Raw JSON textarea for power users

#### 4. Actions
- **Execute Now**: POST to `/api/rpc` (immediate dispatch)
- **Schedule...**: Opens scheduling modal (date/time/recurrence)

### API Requirements

**GET /api/services** - List all known services
```json
{
  "services": [
    {
      "name": "climate_comfort_service",
      "description": "Cabin comfort control",
      "version": "1.0.0",
      "address": "localhost:50062",
      "namespaces": [
        {
          "name": "climate_comfort",
          "methods": [
            {
              "name": "set_comfort_level",
              "description": "Set cabin comfort level",
              "is_schedulable": true,
              "input_parameters": [
                {
                  "name": "settings",
                  "type": "STRUCT",
                  "struct_type": "comfort_settings_t"
                }
              ]
            }
          ]
        }
      ],
      "struct_definitions": {
        "comfort_settings_t": {
          "members": [
            {"name": "target_level", "type": "comfort_level_t"},
            {"name": "zones", "type": "zone_t[]"},
            {"name": "options", "type": "comfort_options_t"}
          ]
        }
      },
      "enum_definitions": {
        "comfort_level_t": {
          "options": [
            {"name": "COOL", "value": 0},
            {"name": "COZY", "value": 1},
            {"name": "WARM", "value": 2}
          ]
        }
      }
    }
  ]
}
```

---

## Facet 2: Calendar View

**Purpose:** Schedule jobs on a calendar with Fleet → Vehicle → Calendar navigation.

**Reference:** `ifex-core/reference-services/scheduler/gui/scheduler_calendar_gui.html`

### UI Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Calendar                                           Jan 5-11, 2026   [+]   │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────┐  ┌────────────────────────────────────────────┐  │
│  │ Fleet                │  │ < >  January 2026         [Day][Week][Month]  │
│  ├──────────────────────┤  ├────────────────────────────────────────────┤  │
│  │ ▼ Fleet-EU-001       │  │       Sun    Mon    Tue    Wed    Thu    ...│  │
│  │   ├─ VIN001 (online) │  │  8:00                                       │  │
│  │   ├─ VIN002 (offline)│  │  9:00  ┌───────────────┐                    │  │
│  │   └─ VIN003 (online) │  │        │ Preheat Cabin │                    │  │
│  │ ▶ Fleet-US-001       │  │        │ climate_comfort │                  │  │
│  │ ▶ Fleet-APAC-001     │  │        └───────────────┘                    │  │
│  │                      │  │ 10:00                                       │  │
│  │ ──────────────────── │  │ 11:00                  ┌───────────────┐    │  │
│  │ Selected: VIN001     │  │                        │ Battery Check │    │  │
│  │ Status: Online       │  │                        │ vehicle_service│   │  │
│  │ Services: 4          │  │                        └───────────────┘    │  │
│  │ Jobs: 2              │  │ 12:00                                       │  │
│  └──────────────────────┘  └────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Navigation Hierarchy

```
Fleets (sidebar)
  └─ Fleet-EU-001
       ├─ VIN00000000000001 (online)
       ├─ VIN00000000000002 (offline)
       └─ VIN00000000000003 (online)
```

### Calendar Features

1. **Week/Day/Month views**
2. **Event color by service** (consistent palette)
3. **Click slot to schedule** (opens service picker + form)
4. **Click event to edit/delete**
5. **Recurring event indicators**
6. **Offline vehicle indicator** (greyed, queued badge)

### Scheduling Modal

When user clicks a time slot:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Schedule Service                                                     [x]  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  When                                                                       │
│  ┌─────────────────────┐  ┌─────────────────────┐                          │
│  │ 2026-01-06          │  │ 09:00              │                           │
│  └─────────────────────┘  └─────────────────────┘                          │
│                                                                             │
│  Service                                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ ▶ climate_comfort_service                                           │   │
│  │     ├─ set_comfort_level (schedulable)                              │   │
│  │     └─ configure_zones (schedulable)                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Parameters (for set_comfort_level)                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ [Dynamic form generated from IFEX schema]                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  Wake Policy                                                                │
│  ( ) No Wake - Only run if vehicle awake                                   │
│  (•) Wake Required - Wake vehicle for this job                             │
│      Wake lead time: [300] seconds before                                  │
│                                                                             │
│  Sleep Policy                                                               │
│  (•) Normal - Allow sleep during execution                                 │
│  ( ) Inhibit - Keep awake until job completes                              │
│                                                                             │
│  Repeat                                                                     │
│  [Does not repeat ▼]  [Daily] [Weekdays] [Weekly] [Custom...]             │
│                                                                             │
│                                                    [Cancel]  [Schedule]    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Offline Vehicle Handling

When a vehicle is offline:
1. Jobs are stored in `offboard_calendar` table
2. Visual indicator: "Queued - will sync when online"
3. On vehicle connect → sync pending calendar items

---

## Facet 3: Dispatch Now

**Purpose:** Execute service methods immediately with minimal friction.

### UI Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Dispatch Now                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Vehicle: [VIN00000000000001        ▼]  Status: ● Online                   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ Quick Actions                                                        │   │
│  │                                                                       │   │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐            │   │
│  │  │   Preheat     │  │   Defrost     │  │  Lock All     │            │   │
│  │  │   Cabin       │  │   Windows     │  │   Doors       │            │   │
│  │  │   climate     │  │   defrost     │  │   vehicle     │            │   │
│  │  └───────────────┘  └───────────────┘  └───────────────┘            │   │
│  │                                                                       │   │
│  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐            │   │
│  │  │   Flash       │  │   Honk        │  │  Get          │            │   │
│  │  │   Lights      │  │   Horn        │  │  Location     │            │   │
│  │  │   vehicle     │  │   vehicle     │  │  vehicle      │            │   │
│  │  └───────────────┘  └───────────────┘  └───────────────┘            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  Custom Command                                                             │
│  Service: [climate_comfort_service ▼]  Method: [set_comfort_level ▼]       │
│                                                                             │
│  Parameters:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ [Dynamic form from selected method]                                  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│                                                          [Execute]          │
│                                                                             │
│  ─────────────────────────────────────────────────────────────────────────  │
│                                                                             │
│  Recent Commands                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 09:15 climate.set_comfort_level({level: COZY})     ✓ Success        │   │
│  │ 09:10 defrost.start_defrost({duration: 300})       ✓ Success        │   │
│  │ 09:05 vehicle.get_status()                         ✗ Timeout        │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Features

1. **Quick Actions Grid** - Common operations with one click
2. **Custom Command** - Full service/method/parameter selection
3. **Recent Commands** - History with status
4. **Offline handling** - Show warning, offer to schedule instead

---

## Database Schema Updates

### New Table: `offboard_calendar`

Stores scheduled jobs created in the cloud before sync to vehicle:

```sql
CREATE TABLE offboard_calendar (
    id SERIAL PRIMARY KEY,
    vehicle_id VARCHAR(64) NOT NULL,
    job_id VARCHAR(64) UNIQUE NOT NULL,

    -- Job definition
    title VARCHAR(255),
    service_name VARCHAR(128) NOT NULL,
    method_name VARCHAR(128) NOT NULL,
    parameters JSONB,

    -- Schedule
    scheduled_time TIMESTAMP WITH TIME ZONE,
    recurrence_rule VARCHAR(255),  -- iCal RRULE
    end_time TIMESTAMP WITH TIME ZONE,

    -- Wake/Sleep policies
    wake_policy SMALLINT DEFAULT 0,      -- 0=NO_WAKE, 1=WAKE_REQUIRED
    sleep_policy SMALLINT DEFAULT 0,     -- 0=SLEEP_NORMAL, 1=INHIBIT_UNTIL_COMPLETE
    wake_lead_time_s INTEGER DEFAULT 0,

    -- Sync status
    sync_status VARCHAR(20) DEFAULT 'pending',  -- pending, synced, failed
    synced_at TIMESTAMP WITH TIME ZONE,
    sync_error TEXT,

    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by VARCHAR(128),  -- User/API that created

    FOREIGN KEY (vehicle_id) REFERENCES vehicles(vehicle_id)
);

CREATE INDEX idx_offboard_calendar_vehicle ON offboard_calendar(vehicle_id);
CREATE INDEX idx_offboard_calendar_sync_status ON offboard_calendar(sync_status);
CREATE INDEX idx_offboard_calendar_scheduled ON offboard_calendar(scheduled_time);
```

### New Table: `service_schemas`

Cache IFEX schemas for offline form generation:

```sql
CREATE TABLE service_schemas (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(128) NOT NULL,
    version VARCHAR(32),

    -- Full IFEX schema (JSON)
    schema_json JSONB NOT NULL,

    -- Extracted for quick lookup
    methods JSONB,           -- Array of method definitions
    struct_definitions JSONB,
    enum_definitions JSONB,

    -- Metadata
    source VARCHAR(32),      -- 'vehicle_sync' or 'manual'
    vehicle_id VARCHAR(64),  -- Source vehicle (if from sync)
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(service_name, version)
);

CREATE INDEX idx_service_schemas_name ON service_schemas(service_name);
```

### Update: `vehicles` table

Add online status (if not already present):

```sql
ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS is_online BOOLEAN DEFAULT false;
ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP WITH TIME ZONE;
```

---

## Sync Strategy

### Cloud → Vehicle Sync (When Vehicle Connects)

```
Vehicle connects (is_online = true)
         │
         ▼
┌─────────────────────────────────┐
│ mqtt_kafka_bridge detects       │
│ v2c/{vehicle_id}/is_online = 1  │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Query offboard_calendar         │
│ WHERE vehicle_id = $1           │
│   AND sync_status = 'pending'   │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ For each pending job:           │
│ 1. Build scheduler_command_t    │
│    (COMMAND_CREATE_JOB)         │
│ 2. Publish to c2v/{vid}/202     │
│ 3. Update sync_status='synced'  │
└─────────────────────────────────┘
```

### Vehicle → Cloud Sync (Scheduler Mirror)

Existing flow - vehicle publishes sync messages, `scheduler_mirror` updates `jobs` table.

### Conflict Resolution

When vehicle and cloud both have jobs:
1. **Vehicle is source of truth** for execution state
2. **Cloud is source of truth** for pending (not-yet-synced) jobs
3. On sync, cloud jobs are sent with `CREATE_JOB` or `UPDATE_JOB`
4. Vehicle acknowledges with `scheduler_command_ack_t`
5. Cloud updates `offboard_calendar.sync_status` on ack

---

## API Additions

### Service Discovery

**GET /api/services**
```json
{
  "services": [...]  // As defined above
}
```

**GET /api/services/{name}**
```json
{
  "name": "climate_comfort_service",
  "namespaces": [...],
  "struct_definitions": {...},
  "enum_definitions": {...}
}
```

**GET /api/vehicles/{id}/services**
```json
{
  "services": [...]  // Services registered by this vehicle
}
```

### Calendar

**GET /api/calendar/{vehicle_id}**
```json
{
  "jobs": [...],           // From 'jobs' table (synced from vehicle)
  "pending": [...],        // From 'offboard_calendar' (not yet synced)
  "vehicle_online": true
}
```

**POST /api/calendar/{vehicle_id}**
```json
{
  "title": "Morning preheat",
  "service_name": "climate_comfort_service",
  "method_name": "set_comfort_level",
  "parameters": {"level": "COZY"},
  "scheduled_time": "2026-01-06T07:00:00Z",
  "recurrence_rule": "FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR",
  "wake_policy": 1,
  "wake_lead_time_s": 300
}
```

Response:
```json
{
  "job_id": "uuid...",
  "sync_status": "synced",        // or "pending" if vehicle offline
  "scheduled_time": "2026-01-06T07:00:00Z"
}
```

---

## Implementation Order

### Phase 1: Schema & API Foundation
1. Add `service_schemas` table
2. Add `offboard_calendar` table
3. Extend fleet_api.py with new endpoints
4. Populate service_schemas from vehicle discovery sync

### Phase 2: Service Explorer Facet
1. Create service explorer HTML/JS component
2. Implement dynamic form builder
3. Wire up Execute Now and Schedule actions

### Phase 3: Calendar View Facet
1. Create calendar component with Fleet→Vehicle navigation
2. Implement week/day/month views
3. Add scheduling modal with dynamic forms
4. Add wake/sleep policy controls

### Phase 4: Dispatch Now Facet
1. Create quick actions grid
2. Implement custom command section
3. Add recent commands history

### Phase 5: Offline Sync
1. Implement vehicle online detection
2. Add pending job sync on connect
3. Add sync status indicators in UI

---

## Shared Components

### VehicleSelector
```javascript
// Dropdown with online/offline status
<VehicleSelector
  fleetId="EU-001"
  onChange={setVehicle}
  showStatus={true}
/>
```

### ServiceMethodPicker
```javascript
// Nested service → method selection
<ServiceMethodPicker
  vehicleId={vehicleId}
  onSelect={(service, method) => {...}}
  filterSchedulable={true}
/>
```

### DynamicFormBuilder
```javascript
// Generates form from IFEX schema
<DynamicFormBuilder
  parameters={method.input_parameters}
  structDefs={service.struct_definitions}
  enumDefs={service.enum_definitions}
  onChange={setFormData}
/>
```

### WakeSleepPolicyPicker
```javascript
// Wake/sleep policy selection
<WakeSleepPolicyPicker
  wakePolicy={wakePolicy}
  sleepPolicy={sleepPolicy}
  wakeLeadTime={wakeLeadTime}
  onChange={setPolicies}
/>
```

---

## Technology Choices

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Frontend | Vanilla JS + HTML | Match existing pattern, no build step |
| Styling | CSS (inline) | Single-file deployment |
| Backend | Python/Flask | Match existing fleet_api.py |
| Calendar | Custom component | Calendar libraries add complexity |
| Forms | Dynamic generation | Must handle arbitrary IFEX schemas |

Future consideration: React/Vue if complexity grows.
