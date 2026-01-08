#!/usr/bin/env python3
"""
IFEX Fleet Dashboard API
REST API for fleet management dashboard - queries PostgreSQL for vehicle/service data
Also provides RPC and Scheduler APIs for cloud-to-vehicle commands
"""

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import psycopg2
import psycopg2.extras
import paho.mqtt.client as mqtt
import os
import uuid
import json
import time
import threading
from datetime import datetime

app = Flask(__name__)
CORS(app)

# MQTT client for c2v publishing
mqtt_client = None
mqtt_connected = False

# Database connection settings
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'ifex_offboard')
DB_USER = os.getenv('POSTGRES_USER', 'ifex')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'ifex_dev')

# MQTT connection settings
MQTT_HOST = os.getenv('MQTT_HOST', 'localhost')
MQTT_PORT = int(os.getenv('MQTT_PORT', '1883'))


def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    """MQTT connection callback (paho-mqtt v2 API)"""
    global mqtt_connected
    if reason_code == 0:
        mqtt_connected = True
        print(f"Connected to MQTT broker at {MQTT_HOST}:{MQTT_PORT}")
    else:
        mqtt_connected = False
        print(f"Failed to connect to MQTT: {reason_code}")


def on_mqtt_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    """MQTT disconnect callback (paho-mqtt v2 API)"""
    global mqtt_connected
    mqtt_connected = False
    print(f"Disconnected from MQTT: {reason_code}")


def init_mqtt():
    """Initialize MQTT client for c2v publishing"""
    global mqtt_client
    mqtt_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"fleet-dashboard-{uuid.uuid4().hex[:8]}"
    )
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_disconnect = on_mqtt_disconnect
    try:
        mqtt_client.connect(MQTT_HOST, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        print(f"Failed to connect to MQTT: {e}")


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


@app.route('/')
def serve_dashboard():
    """Serve the dashboard HTML"""
    return send_file('fleet_dashboard.html')


@app.route('/api/stats')
def get_stats():
    """Get fleet statistics"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    stats = {}

    # Total vehicles with enrichment
    cur.execute("SELECT COUNT(*) as count FROM vehicle_enrichment")
    stats['total_vehicles'] = cur.fetchone()['count']

    # Vehicles with registered services (using vehicle_schemas)
    cur.execute("SELECT COUNT(DISTINCT vehicle_id) as count FROM vehicle_schemas")
    stats['active_vehicles'] = cur.fetchone()['count']

    # Total unique services (from schema_registry)
    cur.execute("SELECT COUNT(*) as count FROM schema_registry")
    stats['total_services'] = cur.fetchone()['count']

    # Total jobs
    cur.execute("SELECT COUNT(*) as count FROM jobs")
    stats['total_jobs'] = cur.fetchone()['count']

    # Vehicles by region
    cur.execute("""
        SELECT region, COUNT(*) as count
        FROM vehicle_enrichment
        GROUP BY region
        ORDER BY count DESC
    """)
    stats['by_region'] = cur.fetchall()

    # Vehicles by fleet
    cur.execute("""
        SELECT fleet_id, COUNT(*) as count
        FROM vehicle_enrichment
        GROUP BY fleet_id
        ORDER BY count DESC
        LIMIT 10
    """)
    stats['by_fleet'] = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify(stats)


@app.route('/api/vehicles')
def get_vehicles():
    """Get vehicles with enrichment and service count"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Pagination
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    offset = (page - 1) * per_page

    # Filters
    fleet_filter = request.args.get('fleet')
    region_filter = request.args.get('region')
    search = request.args.get('search')

    # Build query (using vehicle_schemas for service count)
    query = """
        SELECT
            e.vehicle_id,
            e.fleet_id,
            e.region,
            e.model,
            e.year,
            e.owner,
            e.created_at,
            e.updated_at,
            v.is_online,
            COALESCE(s.service_count, 0) as service_count,
            COALESCE(j.job_count, 0) as job_count
        FROM vehicle_enrichment e
        LEFT JOIN vehicles v ON e.vehicle_id = v.vehicle_id
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as service_count
            FROM vehicle_schemas
            GROUP BY vehicle_id
        ) s ON e.vehicle_id = s.vehicle_id
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as job_count
            FROM jobs
            GROUP BY vehicle_id
        ) j ON e.vehicle_id = j.vehicle_id
        WHERE 1=1
    """
    params = []

    if fleet_filter:
        query += " AND e.fleet_id = %s"
        params.append(fleet_filter)

    if region_filter:
        query += " AND e.region = %s"
        params.append(region_filter)

    if search:
        query += " AND e.vehicle_id ILIKE %s"
        params.append(f'%{search}%')

    # Get total count
    count_query = f"SELECT COUNT(*) FROM ({query}) subq"
    cur.execute(count_query, params)
    total = cur.fetchone()['count']

    # Get page
    query += " ORDER BY e.vehicle_id LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    cur.execute(query, params)
    vehicles = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({
        'vehicles': vehicles,
        'total': total,
        'page': page,
        'per_page': per_page,
        'pages': (total + per_page - 1) // per_page
    })


@app.route('/api/vehicles/<vehicle_id>')
def get_vehicle(vehicle_id):
    """Get detailed vehicle information"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get enrichment
    cur.execute("""
        SELECT * FROM vehicle_enrichment WHERE vehicle_id = %s
    """, (vehicle_id,))
    enrichment = cur.fetchone()

    if not enrichment:
        cur.close()
        conn.close()
        return jsonify({'error': 'Vehicle not found'}), 404

    # Get services (using vehicle_services_view)
    cur.execute("""
        SELECT service_name, version, schema_hash, methods, last_seen_at
        FROM vehicle_services_view
        WHERE vehicle_id = %s
        ORDER BY service_name
    """, (vehicle_id,))
    services = cur.fetchall()

    # Get jobs
    cur.execute("""
        SELECT job_id, title, service_name, method_name, status, next_run_time
        FROM jobs
        WHERE vehicle_id = %s
        ORDER BY job_id
    """, (vehicle_id,))
    jobs = cur.fetchall()

    # Get recent job executions
    cur.execute("""
        SELECT job_id, status, executed_at_ms, duration_ms, error_message
        FROM job_executions
        WHERE vehicle_id = %s
        ORDER BY executed_at_ms DESC
        LIMIT 10
    """, (vehicle_id,))
    executions = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({
        'vehicle_id': vehicle_id,
        'enrichment': enrichment,
        'services': services,
        'jobs': jobs,
        'recent_executions': executions
    })


@app.route('/api/services')
def get_services():
    """Get all services across fleet (using schema_registry)"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Get unique services from schema_registry with vehicle counts
    cur.execute("""
        SELECT
            sr.service_name,
            sr.version,
            sr.schema_hash,
            sr.schema_count as vehicle_count,
            sr.methods
        FROM schema_registry sr
        ORDER BY sr.schema_count DESC, sr.service_name
    """)
    services = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({'services': services})


@app.route('/api/fleets')
def get_fleets():
    """Get fleet summary (using vehicle_schemas)"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            e.fleet_id,
            e.region,
            COUNT(DISTINCT e.vehicle_id) as vehicle_count,
            COALESCE(SUM(s.service_count), 0) as total_services,
            SUM(CASE WHEN v.is_online THEN 1 ELSE 0 END) as online_count
        FROM vehicle_enrichment e
        LEFT JOIN vehicles v ON e.vehicle_id = v.vehicle_id
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as service_count
            FROM vehicle_schemas
            GROUP BY vehicle_id
        ) s ON e.vehicle_id = s.vehicle_id
        GROUP BY e.fleet_id, e.region
        ORDER BY vehicle_count DESC
    """)
    fleets = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({'fleets': fleets})


@app.route('/api/regions')
def get_regions():
    """Get regions list"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT DISTINCT region FROM vehicle_enrichment ORDER BY region
    """)
    regions = [r['region'] for r in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({'regions': regions})


# =============================================================================
# RPC API - Dispatch remote procedure calls to vehicles
# =============================================================================

@app.route('/api/rpc', methods=['POST'])
def dispatch_rpc():
    """
    Dispatch an RPC call to a vehicle via MQTT

    Request body:
    {
        "vehicle_id": "VIN00000000000001",
        "service_name": "echo_service",
        "method_name": "echo",
        "parameters": {"message": "Hello"},
        "timeout_ms": 5000
    }

    Note: RPCs are fire-and-forget from dashboard perspective.
    Response tracking is handled by dispatcher_api service.
    """
    global mqtt_client, mqtt_connected

    if not mqtt_connected:
        return jsonify({'error': 'MQTT not connected'}), 503

    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    required = ['vehicle_id', 'service_name', 'method_name']
    for field in required:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400

    vehicle_id = data['vehicle_id']
    service_name = data['service_name']
    method_name = data['method_name']
    parameters = data.get('parameters', {})
    timeout_ms = data.get('timeout_ms', 5000)

    # Generate request ID
    request_id = str(uuid.uuid4())
    timestamp_ns = int(time.time() * 1e9)

    # Build RPC request envelope (matches dispatcher_rpc_envelope.proto)
    rpc_request = {
        'request_id': request_id,
        'service_name': service_name,
        'method_name': method_name,
        'parameters_json': json.dumps(parameters),
        'timeout_ms': timeout_ms,
        'timestamp_ns': timestamp_ns
    }

    # Publish to MQTT c2v topic (content_id=200 for RPC)
    topic = f"c2v/{vehicle_id}/200"
    try:
        result = mqtt_client.publish(topic, json.dumps(rpc_request), qos=1)
        result.wait_for_publish(timeout=2.0)
    except Exception as e:
        return jsonify({
            'error': f'MQTT publish failed: {str(e)}',
            'request_id': request_id
        }), 500

    return jsonify({
        'request_id': request_id,
        'vehicle_id': vehicle_id,
        'service_name': service_name,
        'method_name': method_name,
        'status': 'dispatched',
        'topic': topic
    })


# Note: RPC status tracking removed - RPCs are now fire-and-forget from dashboard.
# For synchronous RPC with response tracking, use the dispatcher_api gRPC service.


# =============================================================================
# Scheduler API - Create/manage scheduled jobs on vehicles
# =============================================================================

@app.route('/api/schedule', methods=['POST'])
def schedule_job():
    """
    Schedule a job on a vehicle

    Request body:
    {
        "vehicle_id": "VIN00000000000001",
        "title": "Hourly temperature check",
        "service_name": "climate_service",
        "method_name": "get_temperature",
        "parameters": {"zone": "all"},
        "scheduled_time": "2024-01-15T10:00:00Z",
        "recurrence_rule": "FREQ=HOURLY;INTERVAL=1"
    }
    """
    global mqtt_client, mqtt_connected

    if not mqtt_connected:
        return jsonify({'error': 'MQTT not connected'}), 503

    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    required = ['vehicle_id', 'service_name', 'method_name']
    for field in required:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400

    vehicle_id = data['vehicle_id']
    job_id = data.get('job_id', str(uuid.uuid4()))

    # Build scheduler command message
    scheduler_cmd = {
        'command': 'create_job',
        'job': {
            'job_id': job_id,
            'title': data.get('title', f"{data['service_name']}.{data['method_name']}"),
            'service': data['service_name'],
            'method': data['method_name'],
            'parameters': json.dumps(data.get('parameters', {})),
            'scheduled_time': data.get('scheduled_time', ''),
            'recurrence_rule': data.get('recurrence_rule', ''),
            'status': 0,  # PENDING
            'created_at_ms': int(time.time() * 1000)
        }
    }

    # Publish to MQTT c2v topic (content_id=202 for Scheduler)
    topic = f"c2v/{vehicle_id}/202"
    try:
        result = mqtt_client.publish(topic, json.dumps(scheduler_cmd), qos=1)
        result.wait_for_publish(timeout=2.0)
    except Exception as e:
        return jsonify({'error': f'MQTT publish failed: {str(e)}'}), 500

    return jsonify({
        'job_id': job_id,
        'vehicle_id': vehicle_id,
        'status': 'scheduled',
        'topic': topic
    })


@app.route('/api/schedule/<vehicle_id>/<job_id>', methods=['DELETE'])
def delete_job(vehicle_id, job_id):
    """Delete a scheduled job on a vehicle"""
    global mqtt_client, mqtt_connected

    if not mqtt_connected:
        return jsonify({'error': 'MQTT not connected'}), 503

    # Build delete command
    scheduler_cmd = {
        'command': 'delete_job',
        'job_id': job_id
    }

    topic = f"c2v/{vehicle_id}/202"
    try:
        result = mqtt_client.publish(topic, json.dumps(scheduler_cmd), qos=1)
        result.wait_for_publish(timeout=2.0)
    except Exception as e:
        return jsonify({'error': f'MQTT publish failed: {str(e)}'}), 500

    return jsonify({
        'job_id': job_id,
        'vehicle_id': vehicle_id,
        'status': 'delete_requested'
    })


@app.route('/api/jobs')
def get_all_jobs():
    """Get all jobs across fleet with optional filters"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    vehicle_id = request.args.get('vehicle_id')
    status = request.args.get('status')
    limit = int(request.args.get('limit', 100))

    query = "SELECT * FROM jobs WHERE 1=1"
    params = []

    if vehicle_id:
        query += " AND vehicle_id = %s"
        params.append(vehicle_id)

    if status:
        query += " AND status = %s"
        params.append(status)

    query += " ORDER BY sync_updated_at DESC LIMIT %s"
    params.append(limit)

    cur.execute(query, params)
    jobs = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({'jobs': jobs, 'count': len(jobs)})


@app.route('/api/jobs/<vehicle_id>/<job_id>/executions')
def get_job_executions(vehicle_id, job_id):
    """Get execution history for a specific job"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    limit = int(request.args.get('limit', 20))

    cur.execute("""
        SELECT * FROM job_executions
        WHERE vehicle_id = %s AND job_id = %s
        ORDER BY executed_at_ms DESC
        LIMIT %s
    """, (vehicle_id, job_id, limit))
    executions = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({'executions': executions})


# =============================================================================
# Health and Status
# =============================================================================

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    status = {
        'status': 'healthy',
        'mqtt_connected': mqtt_connected,
        'timestamp': datetime.now().isoformat()
    }

    # Check database
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        status['database'] = 'connected'
    except Exception as e:
        status['database'] = f'error: {str(e)}'
        status['status'] = 'degraded'

    return jsonify(status)


# =============================================================================
# Service Schemas API - For dynamic form generation
# Queries pre-parsed JSONB from schema_registry table (hash-based deduplication)
# =============================================================================


@app.route('/api/schemas')
def get_schemas():
    """
    Get all service schemas for UI form generation
    Uses fleet_services_view which aggregates unique schemas across fleet
    """
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            service_name,
            version,
            schema_hash,
            methods,
            struct_definitions,
            enum_definitions,
            vehicle_count,
            first_seen_at
        FROM fleet_services_view
        ORDER BY service_name
    """)
    schemas_raw = cur.fetchall()

    cur.close()
    conn.close()

    schemas = []
    for row in schemas_raw:
        schemas.append({
            'service_name': row['service_name'],
            'version': row['version'] or '1.0.0',
            'schema_hash': row['schema_hash'],
            'methods': row['methods'] or [],
            'struct_definitions': row['struct_definitions'] or {},
            'enum_definitions': row['enum_definitions'] or {},
            'vehicle_count': row['vehicle_count'],
            'source': 'discovery'
        })

    return jsonify({'schemas': schemas})


@app.route('/api/schemas/<service_name>')
def get_schema(service_name):
    """Get specific service schema with full details including methods and types"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            service_name,
            version,
            schema_hash,
            ifex_schema,
            methods,
            struct_definitions,
            enum_definitions,
            schema_count as vehicle_count
        FROM schema_registry
        WHERE service_name = %s
        LIMIT 1
    """, (service_name,))
    schema = cur.fetchone()

    cur.close()
    conn.close()

    if not schema:
        return jsonify({'error': 'Schema not found'}), 404

    return jsonify({
        'service_name': schema['service_name'],
        'version': schema['version'] or '1.0.0',
        'schema_hash': schema['schema_hash'],
        'ifex_schema': schema['ifex_schema'],
        'methods': schema['methods'] or [],
        'struct_definitions': schema['struct_definitions'] or {},
        'enum_definitions': schema['enum_definitions'] or {},
        'vehicle_count': schema['vehicle_count'],
        'source': 'discovery'
    })


@app.route('/api/discovered-services')
def get_discovered_services():
    """Get services discovered from vehicles (for Services tab)"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    vehicle_id = request.args.get('vehicle_id')

    if vehicle_id:
        # Services for specific vehicle
        cur.execute("""
            SELECT
                vs.service_name,
                vs.version,
                vs.schema_hash,
                vs.methods,
                vs.last_seen_at,
                COALESCE(v.is_online, false) as is_online
            FROM vehicle_services_view vs
            LEFT JOIN vehicles v ON vs.vehicle_id = v.vehicle_id
            WHERE vs.vehicle_id = %s
            ORDER BY vs.service_name
        """, (vehicle_id,))
        services = cur.fetchall()

        cur.close()
        conn.close()

        enriched = []
        for svc in services:
            methods = svc['methods'] or []
            enriched.append({
                'service_name': svc['service_name'],
                'version': svc['version'],
                'schema_hash': svc['schema_hash'],
                'last_seen_at': svc['last_seen_at'].isoformat() if svc['last_seen_at'] else None,
                'is_online': svc['is_online'],
                'method_count': len(methods),
                'has_schema': True
            })
        return jsonify({'services': enriched})

    else:
        # All services grouped from schema_registry
        cur.execute("""
            SELECT
                service_name,
                version,
                schema_hash,
                methods,
                schema_count as vehicle_count,
                first_seen_at as last_seen
            FROM schema_registry
            ORDER BY schema_count DESC
        """)
        services = cur.fetchall()

        cur.close()
        conn.close()

        enriched = []
        for svc in services:
            methods = svc['methods'] or []
            enriched.append({
                'service_name': svc['service_name'],
                'version': svc['version'],
                'schema_hash': svc['schema_hash'],
                'vehicle_count': svc['vehicle_count'],
                'last_seen': svc['last_seen'].isoformat() if svc['last_seen'] else None,
                'method_count': len(methods),
                'has_schema': True
            })
        return jsonify({'services': enriched})


# =============================================================================
# Calendar API - Cloud-side job scheduling with offline support
# =============================================================================

@app.route('/api/calendar/<vehicle_id>')
def get_calendar(vehicle_id):
    """Get calendar events for a vehicle (synced jobs + pending offboard jobs)"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Check if vehicle exists and get online status
    cur.execute("""
        SELECT vehicle_id, is_online, last_seen_at FROM vehicles WHERE vehicle_id = %s
    """, (vehicle_id,))
    vehicle = cur.fetchone()

    if not vehicle:
        cur.close()
        conn.close()
        return jsonify({'error': 'Vehicle not found'}), 404

    # Get synced jobs from vehicle
    cur.execute("""
        SELECT job_id, title, service_name, method_name, parameters,
               scheduled_time, recurrence_rule, next_run_time, status,
               wake_policy, sleep_policy, wake_lead_time_s,
               'synced' as source, sync_updated_at as updated_at
        FROM jobs
        WHERE vehicle_id = %s
        ORDER BY scheduled_time
    """, (vehicle_id,))
    synced_jobs = cur.fetchall()

    # Get pending offboard calendar entries
    cur.execute("""
        SELECT job_id, title, service_name, method_name, parameters,
               scheduled_time::text, recurrence_rule, NULL as next_run_time,
               'pending' as status,
               wake_policy, sleep_policy, wake_lead_time_s,
               'offboard' as source, created_at as updated_at,
               sync_status, sync_error
        FROM offboard_calendar
        WHERE vehicle_id = %s AND sync_status != 'synced'
        ORDER BY scheduled_time
    """, (vehicle_id,))
    pending_jobs = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({
        'vehicle_id': vehicle_id,
        'vehicle_online': vehicle['is_online'],
        'last_seen': vehicle['last_seen_at'].isoformat() if vehicle['last_seen_at'] else None,
        'jobs': synced_jobs,
        'pending': pending_jobs
    })


@app.route('/api/calendar/<vehicle_id>', methods=['POST'])
def create_calendar_entry(vehicle_id):
    """
    Create a calendar entry for a vehicle

    If vehicle is online: sends scheduler command immediately
    If vehicle is offline: stores in offboard_calendar for later sync

    Request body:
    {
        "title": "Morning preheat",
        "service_name": "climate_comfort_service",
        "method_name": "set_comfort_level",
        "parameters": {"level": "COZY"},
        "scheduled_time": "2026-01-06T07:00:00Z",
        "recurrence_rule": "FREQ=DAILY;BYDAY=MO,TU,WE,TH,FR",
        "wake_policy": 1,
        "sleep_policy": 0,
        "wake_lead_time_s": 300
    }
    """
    global mqtt_client, mqtt_connected

    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    required = ['service_name', 'method_name']
    for field in required:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Check if vehicle exists and get online status
    cur.execute("""
        SELECT vehicle_id, is_online FROM vehicles WHERE vehicle_id = %s
    """, (vehicle_id,))
    vehicle = cur.fetchone()

    if not vehicle:
        # Auto-create vehicle record
        cur.execute("""
            INSERT INTO vehicles (vehicle_id) VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (vehicle_id,))
        conn.commit()
        vehicle = {'vehicle_id': vehicle_id, 'is_online': False}

    job_id = data.get('job_id', str(uuid.uuid4()))
    scheduled_time = data.get('scheduled_time')

    # Store in offboard_calendar
    try:
        cur.execute("""
            INSERT INTO offboard_calendar
            (vehicle_id, job_id, title, service_name, method_name, parameters,
             scheduled_time, recurrence_rule, end_time,
             wake_policy, sleep_policy, wake_lead_time_s,
             sync_status, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            vehicle_id,
            job_id,
            data.get('title', f"{data['service_name']}.{data['method_name']}"),
            data['service_name'],
            data['method_name'],
            json.dumps(data.get('parameters', {})),
            scheduled_time,
            data.get('recurrence_rule'),
            data.get('end_time'),
            data.get('wake_policy', 0),
            data.get('sleep_policy', 0),
            data.get('wake_lead_time_s', 0),
            'pending',
            data.get('created_by', 'api')
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({'error': f'Database error: {str(e)}'}), 500

    sync_status = 'pending'
    sync_error = None

    # If vehicle is online and MQTT connected, send immediately
    if vehicle['is_online'] and mqtt_connected:
        scheduler_cmd = {
            'command_id': str(uuid.uuid4()),
            'timestamp_ns': int(time.time() * 1e9),
            'requester_id': 'fleet_dashboard',
            'type': 1,  # COMMAND_CREATE_JOB
            'create_job': {
                'job_id': job_id,
                'title': data.get('title', f"{data['service_name']}.{data['method_name']}"),
                'service': data['service_name'],
                'method': data['method_name'],
                'parameters_json': json.dumps(data.get('parameters', {})),
                'scheduled_time': scheduled_time or '',
                'recurrence_rule': data.get('recurrence_rule', ''),
                'end_time': data.get('end_time', ''),
                'wake_policy': data.get('wake_policy', 0),
                'sleep_policy': data.get('sleep_policy', 0),
                'wake_lead_time_s': data.get('wake_lead_time_s', 0)
            }
        }

        topic = f"c2v/{vehicle_id}/202"
        try:
            result = mqtt_client.publish(topic, json.dumps(scheduler_cmd), qos=1)
            result.wait_for_publish(timeout=2.0)
            sync_status = 'synced'

            # Update offboard_calendar sync status
            cur.execute("""
                UPDATE offboard_calendar
                SET sync_status = 'synced', synced_at = NOW()
                WHERE job_id = %s
            """, (job_id,))
            conn.commit()

        except Exception as e:
            sync_error = str(e)
            sync_status = 'pending'

    cur.close()
    conn.close()

    return jsonify({
        'job_id': job_id,
        'vehicle_id': vehicle_id,
        'vehicle_online': vehicle['is_online'],
        'sync_status': sync_status,
        'sync_error': sync_error,
        'scheduled_time': scheduled_time
    })


@app.route('/api/calendar/<vehicle_id>/<job_id>', methods=['DELETE'])
def delete_calendar_entry(vehicle_id, job_id):
    """Delete a calendar entry"""
    global mqtt_client, mqtt_connected

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Check if it's in offboard_calendar (not yet synced)
    cur.execute("""
        SELECT id, sync_status FROM offboard_calendar
        WHERE vehicle_id = %s AND job_id = %s
    """, (vehicle_id, job_id))
    offboard_entry = cur.fetchone()

    if offboard_entry and offboard_entry['sync_status'] == 'pending':
        # Just delete from offboard_calendar
        cur.execute("""
            DELETE FROM offboard_calendar WHERE job_id = %s
        """, (job_id,))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'job_id': job_id, 'status': 'deleted', 'source': 'offboard'})

    # Check vehicle online status
    cur.execute("""
        SELECT is_online FROM vehicles WHERE vehicle_id = %s
    """, (vehicle_id,))
    vehicle = cur.fetchone()

    cur.close()
    conn.close()

    if not vehicle:
        return jsonify({'error': 'Vehicle not found'}), 404

    # Send delete command to vehicle
    if mqtt_connected:
        scheduler_cmd = {
            'command_id': str(uuid.uuid4()),
            'timestamp_ns': int(time.time() * 1e9),
            'requester_id': 'fleet_dashboard',
            'type': 3,  # COMMAND_DELETE_JOB
            'delete_job_id': job_id
        }

        topic = f"c2v/{vehicle_id}/202"
        try:
            result = mqtt_client.publish(topic, json.dumps(scheduler_cmd), qos=1)
            result.wait_for_publish(timeout=2.0)
            return jsonify({
                'job_id': job_id,
                'vehicle_id': vehicle_id,
                'status': 'delete_sent',
                'vehicle_online': vehicle['is_online']
            })
        except Exception as e:
            return jsonify({'error': f'MQTT publish failed: {str(e)}'}), 500

    return jsonify({'error': 'MQTT not connected'}), 503


if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    print(f"Starting IFEX Fleet Dashboard API on port {port}")
    print(f"Database: {DB_NAME}@{DB_HOST}:{DB_PORT}")
    print(f"MQTT: {MQTT_HOST}:{MQTT_PORT}")

    # Initialize MQTT for c2v publishing
    init_mqtt()

    print(f"Open http://localhost:{port}/ in your browser")
    app.run(host='0.0.0.0', port=port, debug=True)
