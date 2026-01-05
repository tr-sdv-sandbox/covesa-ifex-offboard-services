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

    # Vehicles with registered services
    cur.execute("SELECT COUNT(DISTINCT vehicle_id) as count FROM services")
    stats['active_vehicles'] = cur.fetchone()['count']

    # Total services
    cur.execute("SELECT COUNT(*) as count FROM services")
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

    # Build query
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
            COALESCE(s.service_count, 0) as service_count,
            COALESCE(j.job_count, 0) as job_count
        FROM vehicle_enrichment e
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as service_count
            FROM services
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

    # Get services
    cur.execute("""
        SELECT service_name, version, status, endpoint_address, updated_at
        FROM services
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
    """Get all services across fleet"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Group by service name
    cur.execute("""
        SELECT
            service_name,
            COUNT(DISTINCT vehicle_id) as vehicle_count,
            COUNT(*) as instance_count
        FROM services
        GROUP BY service_name
        ORDER BY vehicle_count DESC
    """)
    services = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({'services': services})


@app.route('/api/fleets')
def get_fleets():
    """Get fleet summary"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT
            e.fleet_id,
            e.region,
            COUNT(DISTINCT e.vehicle_id) as vehicle_count,
            COALESCE(SUM(s.service_count), 0) as total_services
        FROM vehicle_enrichment e
        LEFT JOIN (
            SELECT vehicle_id, COUNT(*) as service_count
            FROM services
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
    Dispatch an RPC call to a vehicle

    Request body:
    {
        "vehicle_id": "VIN00000000000001",
        "service_name": "climate_service",
        "method_name": "set_temperature",
        "parameters": {"zone": "driver", "temperature": 22.0},
        "timeout_ms": 5000
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
    service_name = data['service_name']
    method_name = data['method_name']
    parameters = data.get('parameters', {})
    timeout_ms = data.get('timeout_ms', 5000)

    # Generate correlation ID
    correlation_id = str(uuid.uuid4())
    timestamp_ns = int(time.time() * 1e9)

    # Build RPC request message (JSON for now, can be protobuf later)
    rpc_request = {
        'correlation_id': correlation_id,
        'service_name': service_name,
        'method_name': method_name,
        'parameters_json': json.dumps(parameters),
        'timeout_ms': timeout_ms,
        'request_timestamp_ns': timestamp_ns
    }

    # Store request in database
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO rpc_requests
            (correlation_id, vehicle_id, service_name, method_name,
             parameters_json, timeout_ms, request_timestamp_ns, response_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending')
        """, (correlation_id, vehicle_id, service_name, method_name,
              json.dumps(parameters), timeout_ms, timestamp_ns))
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        return jsonify({'error': f'Database error: {str(e)}'}), 500
    finally:
        cur.close()
        conn.close()

    # Publish to MQTT c2v topic (content_id=200 for RPC)
    topic = f"c2v/{vehicle_id}/200"
    try:
        result = mqtt_client.publish(topic, json.dumps(rpc_request), qos=1)
        result.wait_for_publish(timeout=2.0)
    except Exception as e:
        return jsonify({
            'error': f'MQTT publish failed: {str(e)}',
            'correlation_id': correlation_id
        }), 500

    return jsonify({
        'correlation_id': correlation_id,
        'vehicle_id': vehicle_id,
        'service_name': service_name,
        'method_name': method_name,
        'status': 'dispatched',
        'topic': topic
    })


@app.route('/api/rpc/<correlation_id>')
def get_rpc_status(correlation_id):
    """Get status of an RPC request by correlation ID"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        SELECT * FROM rpc_requests WHERE correlation_id = %s
    """, (correlation_id,))
    result = cur.fetchone()

    cur.close()
    conn.close()

    if not result:
        return jsonify({'error': 'RPC request not found'}), 404

    return jsonify(result)


@app.route('/api/rpc/vehicle/<vehicle_id>')
def get_vehicle_rpc_history(vehicle_id):
    """Get RPC history for a vehicle"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    limit = int(request.args.get('limit', 20))

    cur.execute("""
        SELECT * FROM rpc_requests
        WHERE vehicle_id = %s
        ORDER BY created_at DESC
        LIMIT %s
    """, (vehicle_id, limit))
    results = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({'requests': results})


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


if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    print(f"Starting IFEX Fleet Dashboard API on port {port}")
    print(f"Database: {DB_NAME}@{DB_HOST}:{DB_PORT}")
    print(f"MQTT: {MQTT_HOST}:{MQTT_PORT}")

    # Initialize MQTT for c2v publishing
    init_mqtt()

    print(f"Open http://localhost:{port}/ in your browser")
    app.run(host='0.0.0.0', port=port, debug=True)
