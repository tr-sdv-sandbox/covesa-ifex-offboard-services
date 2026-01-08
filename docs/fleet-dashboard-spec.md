# Fleet Dashboard Specification

Based on the existing `ifex_explorer_gui` in covesa-ifex-core.

## Key Insight: Schema Source

**The IFEX schema comes FROM the vehicle via discovery sync, NOT from a bundled file.**

Each vehicle service:
1. Loads its IFEX YAML schema at startup
2. Computes SHA-256 hash of the schema
3. Discovery sync (content_id=201) transmits hashes to cloud
4. Cloud requests full schema only for unknown hashes
5. Schemas stored deduplicated in PostgreSQL

## Design Goals

- **Bandwidth efficient**: 100K vehicles sending ~100 bytes each (just hashes)
- **Storage efficient**: ~10 unique schemas stored, not 100K copies
- **Parse once**: YAML parsed on first receipt, cached as JSON

## Architecture

### Vehicle Side
```
Service Startup
      │
      ├─▶ Load beverage-service.ifex.yml
      ├─▶ Compute SHA-256 hash
      ├─▶ Register with local Discovery
      │
Backend Transport (discovery sync)
      │
      ├─▶ Collect hashes from Discovery
      ├─▶ Send HashList via v2c/{vid}/201
      │
      ◀── (if cloud requests unknown hash)
      │
      ├─▶ Send SchemaMap via v2c/{vid}/201
```

### Cloud Side
```
mqtt_kafka_bridge
      │
      ├─▶ Receive on v2c/+/201
      ├─▶ Route to Kafka ifex.discovery.201
      │
discovery_mirror
      │
      ├─▶ Consume from Kafka
      ├─▶ For each hash:
      │     Known?  → Link vehicle to existing schema
      │     Unknown? → Send SchemaRequest via c2v/{vid}/201
      │
      ◀── Receive SchemaMap
      │
      ├─▶ Parse YAML, extract name/version/methods/structs/enums
      ├─▶ Store in service_schemas table
      ├─▶ Link vehicle to schema
      │
fleet_api.py
      │
      ├─▶ Query service_schemas (pre-parsed JSON)
      ├─▶ Return to browser
      │
fleet_dashboard.html
      │
      ├─▶ Render dynamic forms from struct/enum definitions
```

## Discovery Protocol (Content ID 201)

Single content_id, bidirectional messaging.

### Wire Format

```protobuf
message discovery_envelope {
    oneof payload {
        HashList manifest = 1;        // v2c: here are my service hashes
        HashList schema_request = 2;  // c2v: send me these
        SchemaMap schemas = 3;        // v2c: here are the schemas
    }
}

message HashList {
    repeated string hashes = 1;       // SHA-256 hex strings
}

message SchemaMap {
    map<string, string> schemas = 1;  // hash → ifex_yaml
}
```

### Protocol Flow

```
VEHICLE                                    CLOUD
   │                                         │
   │  (startup)                              │
   │                                         │
   ├──── HashList ───────────────────────────▶
   │     [abc123, def456, ghi789]            │
   │                                         │
   │                          ┌──────────────┤
   │                          │ Lookup:      │
   │                          │ abc123? ✓    │
   │                          │ def456? ✓    │
   │                          │ ghi789? ✗    │
   │                          └──────────────┤
   │                                         │
   ◀──── HashList (request) ─────────────────┤
   │     [ghi789]                            │
   │                                         │
   ├──── SchemaMap ──────────────────────────▶
   │     {ghi789: "---\nname: climate..."}   │
   │                                         │
   │     Cloud:                              │
   │     - Parse YAML                        │
   │     - Extract service_name, version     │
   │     - Extract methods, structs, enums   │
   │     - Store in service_schemas          │
   │     - Link vehicle_services             │
   │                                         │
   ═══════════════════════════════════════════
   │                                         │
   │  (reconnect - same software)            │
   │                                         │
   ├──── HashList ───────────────────────────▶
   │     [abc123, def456, ghi789]            │
   │                                         │
   │                    All known ───────────┤
   │                    (no response needed) │
   │                                         │

Steady state: 3 hashes ≈ 100 bytes total
```

## Database Schema

```sql
-- Unique schemas (deduplicated by hash)
CREATE TABLE service_schemas (
    schema_hash VARCHAR(64) PRIMARY KEY,  -- SHA-256 hex
    ifex_schema TEXT NOT NULL,            -- Original YAML

    -- Extracted from YAML (cached, avoids re-parsing)
    service_name VARCHAR(128) NOT NULL,
    version VARCHAR(32),
    description TEXT,
    methods JSONB,                        -- [{name, input_parameters, ...}]
    struct_definitions JSONB,             -- {struct_name: {members: [...]}}
    enum_definitions JSONB,               -- {enum_name: {options: [...]}}

    created_at TIMESTAMPTZ DEFAULT NOW(),
    first_seen_vehicle VARCHAR(64)
);

CREATE INDEX idx_schemas_name ON service_schemas(service_name);

-- Vehicle ↔ Schema links (lightweight, many-to-one)
CREATE TABLE vehicle_services (
    vehicle_id VARCHAR(64) NOT NULL REFERENCES vehicles(vehicle_id) ON DELETE CASCADE,
    schema_hash VARCHAR(64) NOT NULL REFERENCES service_schemas(schema_hash),
    last_seen_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (vehicle_id, schema_hash)
);

CREATE INDEX idx_vehicle_services_vehicle ON vehicle_services(vehicle_id);
CREATE INDEX idx_vehicle_services_hash ON vehicle_services(schema_hash);
```

### Replaces existing services table

No migration needed - recreate database with new schema.

## API Endpoints

### GET /api/schemas
Returns all unique service schemas (pre-parsed, from cache).

```python
@app.route('/api/schemas')
def get_schemas():
    """Return all service schemas - already parsed, just query JSON columns"""
    cur.execute("""
        SELECT schema_hash, service_name, version, description,
               methods, struct_definitions, enum_definitions
        FROM service_schemas
        ORDER BY service_name
    """)

    schemas = []
    for row in cur.fetchall():
        schemas.append({
            'schema_hash': row['schema_hash'],
            'service_name': row['service_name'],
            'version': row['version'],
            'description': row['description'],
            'methods': row['methods'],              # Already JSONB
            'struct_definitions': row['struct_definitions'],
            'enum_definitions': row['enum_definitions']
        })

    return jsonify({'schemas': schemas})
```

### GET /api/schemas/{hash}
Returns full schema for a specific hash.

### GET /api/vehicles/{vehicle_id}/services
Returns services available on a specific vehicle.

```python
@app.route('/api/vehicles/<vehicle_id>/services')
def get_vehicle_services(vehicle_id):
    """Return services for a specific vehicle with full schema details"""
    cur.execute("""
        SELECT s.schema_hash, s.service_name, s.version,
               s.methods, s.struct_definitions, s.enum_definitions,
               vs.last_seen_at
        FROM vehicle_services vs
        JOIN service_schemas s ON vs.schema_hash = s.schema_hash
        WHERE vs.vehicle_id = %s
        ORDER BY s.service_name
    """, (vehicle_id,))

    return jsonify({'services': cur.fetchall()})
```

### POST /api/vehicles/{vehicle_id}/rpc
Dispatches RPC to vehicle via MQTT (content_id=200).

## YAML Parsing (discovery_mirror)

Parsing happens **once** when a new schema is received. Based on `ifex_explorer_proxy.py`:

```python
def parse_and_store_schema(schema_hash: str, ifex_yaml: str, vehicle_id: str):
    """Parse IFEX YAML and store with pre-extracted JSON fields"""
    ifex_data = yaml.safe_load(ifex_yaml)

    # Extract service metadata
    service_name = ifex_data.get('name', 'unknown')
    version = f"{ifex_data.get('major_version', 0)}.{ifex_data.get('minor_version', 0)}"
    description = ifex_data.get('description', '')

    # Extract definitions
    struct_defs = extract_struct_definitions(ifex_data)
    enum_defs = extract_enum_definitions(ifex_data)
    methods = extract_methods(ifex_data, enum_defs)

    # Store with pre-parsed JSON columns
    cur.execute("""
        INSERT INTO service_schemas
        (schema_hash, ifex_schema, service_name, version, description,
         methods, struct_definitions, enum_definitions, first_seen_vehicle)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (schema_hash) DO NOTHING
    """, (
        schema_hash, ifex_yaml, service_name, version, description,
        json.dumps(methods), json.dumps(struct_defs), json.dumps(enum_defs),
        vehicle_id
    ))


def extract_struct_definitions(ifex_data):
    """Extract all struct definitions from IFEX YAML"""
    struct_defs = {}
    for ns in ifex_data.get('namespaces', []):
        # Build enum lookup first
        enum_lookup = {e['name']: e for e in ns.get('enumerations', [])}

        for struct in ns.get('structs', []):
            members = []
            for member in struct.get('members', []):
                member_info = {
                    'name': member['name'],
                    'datatype': member['datatype'],
                    'description': member.get('description', ''),
                    'default_value': member.get('default'),
                    'constraints': member.get('constraints', {}),
                    'mandatory': member.get('mandatory', True)
                }

                # If member type is an enum, add values
                if member['datatype'] in enum_lookup:
                    enum_def = enum_lookup[member['datatype']]
                    member_info['enum_values'] = [
                        f"{opt['name']} ({opt['value']})"
                        for opt in enum_def.get('options', [])
                    ]

                members.append(member_info)

            struct_defs[struct['name']] = {
                'description': struct.get('description', ''),
                'members': members
            }

    return struct_defs


def extract_enum_definitions(ifex_data):
    """Extract all enum definitions from IFEX YAML"""
    enum_defs = {}
    for ns in ifex_data.get('namespaces', []):
        for enum_def in ns.get('enumerations', []):
            enum_defs[enum_def['name']] = {
                'description': enum_def.get('description', ''),
                'datatype': enum_def.get('datatype', 'uint8'),
                'options': [
                    {'name': opt['name'], 'value': opt['value']}
                    for opt in enum_def.get('options', [])
                ]
            }
    return enum_defs


def extract_methods(ifex_data, enum_defs):
    """Extract all methods with parameter details"""
    methods = []
    for ns in ifex_data.get('namespaces', []):
        for method in ns.get('methods', []):
            methods.append({
                'name': method['name'],
                'namespace': ns['name'],
                'description': method.get('description', ''),
                'input_parameters': parse_parameters(method.get('input', []), enum_defs),
                'output_parameters': parse_parameters(method.get('output', []), enum_defs),
                'is_schedulable': method.get('x_scheduling', {}).get('enabled', False)
            })
    return methods
```

## UI Form Generation

Based on `ifex_explorer_gui.html`. Data comes pre-parsed from API.

```javascript
function renderFormBuilder(method, structDefinitions, enumDefinitions) {
    const container = document.getElementById('dynamic-form');
    let html = '';

    for (const param of method.input_parameters) {
        html += renderParameter(param, '', structDefinitions, enumDefinitions);
    }

    container.innerHTML = html;
}

function renderParameter(param, prefix, structDefs, enumDefs) {
    const fieldId = prefix ? `${prefix}.${param.name}` : param.name;
    const datatype = param.datatype;

    // Check if it's an enum type
    if (enumDefs[datatype]) {
        return renderEnumField(fieldId, param, enumDefs[datatype]);
    }

    // Check if it's a struct type
    if (structDefs[datatype]) {
        return renderStructField(fieldId, param, structDefs[datatype], structDefs, enumDefs);
    }

    // Check if it's an array type (ends with [])
    if (datatype.endsWith('[]')) {
        const baseType = datatype.slice(0, -2);
        if (enumDefs[baseType]) {
            return renderCheckboxGroup(fieldId, param, enumDefs[baseType]);
        }
        return renderArrayField(fieldId, param);
    }

    // Basic types
    return renderBasicField(fieldId, param, datatype);
}
```

## Data Flow Summary

```
VEHICLE                                    CLOUD
───────                                    ─────
Service starts
    │
    ├─▶ Load beverage-service.ifex.yml
    ├─▶ Compute SHA-256 → abc123
    ├─▶ Register with local Discovery
    │
Backend Transport sync
    │
    ├─▶ v2c/{vid}/201: [abc123, def456]
    │
    │                              mqtt_kafka_bridge
    │                                   │
    │                                   ├─▶ Kafka ifex.discovery.201
    │                                   │
    │                              discovery_mirror
    │                                   │
    │                                   ├─▶ abc123 known? Yes → link
    │                                   ├─▶ def456 known? No
    │                                   │
    ◀─── c2v/{vid}/201: [def456] ───────┤
    │    (request unknown schema)       │
    │                                   │
    ├─▶ v2c/{vid}/201: {def456: yaml}   │
    │                                   │
    │                                   ├─▶ Parse YAML once
    │                                   ├─▶ Store in service_schemas
    │                                   ├─▶ Link vehicle_services
    │                                   │
    │                              PostgreSQL
    │                                   │
    │                              fleet_api.py
    │                                   │
    │                                   ├─▶ GET /api/schemas
    │                                   │   (returns pre-parsed JSONB)
    │                                   │
    │                              Browser
    │                                   │
    │                                   ├─▶ Render forms from
    │                                   │   struct_definitions
    │                                   │   enum_definitions
```

## Implementation Steps

1. **Update proto/discovery-sync-envelope.proto**
   - New message types: HashList, SchemaMap, discovery_envelope

2. **Update Backend Transport (vehicle side)**
   - Compute schema hashes
   - Send HashList on sync
   - Respond to SchemaRequest with SchemaMap

3. **Update discovery_mirror (cloud side)**
   - Handle HashList: lookup, request unknown
   - Handle SchemaMap: parse YAML, store
   - Link vehicles to schemas

4. **Recreate database**
   - New service_schemas table (deduplicated)
   - New vehicle_services table (links)

5. **Update fleet_api.py**
   - Query pre-parsed JSONB columns
   - No YAML parsing at request time

6. **Update fleet_dashboard.html**
   - Use struct_definitions for nested forms
   - Use enum_definitions for dropdowns

## Example API Response

```json
{
  "schemas": [
    {
      "schema_hash": "abc123...",
      "service_name": "beverage_service",
      "version": "1.0",
      "methods": [
        {
          "name": "prepare_beverage",
          "description": "Start beverage preparation",
          "input_parameters": [
            {"name": "config", "datatype": "beverage_config_t"}
          ]
        }
      ],
      "struct_definitions": {
        "beverage_config_t": {
          "members": [
            {"name": "type", "datatype": "beverage_type_t",
             "enum_values": ["COFFEE (0)", "ESPRESSO (1)"]},
            {"name": "temperature_celsius", "datatype": "uint8",
             "constraints": {"min": 60, "max": 95}}
          ]
        }
      },
      "enum_definitions": {
        "beverage_type_t": {
          "options": [
            {"name": "COFFEE", "value": 0},
            {"name": "ESPRESSO", "value": 1}
          ]
        }
      }
    }
  ]
}
```

## Calendar Integration

Same schema information used for scheduling:

1. User selects vehicle → GET /api/vehicles/{vid}/services
2. User selects service → methods shown (filtered by `is_schedulable: true`)
3. User selects method → form rendered from struct/enum definitions
4. User sets schedule (time, recurrence, wake policy)
5. Job stored in `offboard_calendar` with parameters as JSON
6. When vehicle connects, job synced via content_id=202

## Bandwidth Comparison

| Scenario | Before | After |
|----------|--------|-------|
| 100K vehicles, 5 services each | 5GB schema data | 50KB schema data |
| Daily reconnect (same software) | 50MB/day | 500KB/day |
| New software rollout | 5GB transfer | 50KB + 100K×100B = 10MB |
