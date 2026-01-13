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
import grpc
import os
import json
from datetime import datetime, timezone


# =============================================================================
# Time Conversion Utilities
# All scheduler APIs use epoch milliseconds (uint64) as the canonical time format.
# These convenience functions convert to/from ISO8601 strings for human readability.
# =============================================================================

def iso8601_to_epoch_ms(iso_str: str) -> int:
    """Convert ISO8601 datetime string to epoch milliseconds.

    Supports formats: "2024-12-25T10:30:00Z", "2024-12-25T10:30:00.123Z"
    Returns 0 for empty or invalid input.
    """
    if not iso_str:
        return 0
    try:
        # Handle 'Z' suffix and various ISO8601 formats
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def epoch_ms_to_iso8601(epoch_ms: int) -> str:
    """Convert epoch milliseconds to ISO8601 datetime string.

    Returns empty string for 0 input.
    Output format: "2024-12-25T10:30:00.123Z"
    """
    if not epoch_ms:
        return ""
    try:
        dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        # Format with milliseconds
        ms = epoch_ms % 1000
        return dt.strftime('%Y-%m-%dT%H:%M:%S') + f'.{ms:03d}Z'
    except (ValueError, TypeError, OSError):
        return ""

# Import generated gRPC stubs
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'proto_gen'))
from proto_gen import cloud_dispatcher_service_pb2 as dispatcher_pb2
from proto_gen import cloud_dispatcher_service_pb2_grpc as dispatcher_grpc
from proto_gen import cloud_scheduler_service_pb2 as scheduler_pb2
from proto_gen import cloud_scheduler_service_pb2_grpc as scheduler_grpc
from proto_gen import cloud_discovery_service_pb2 as discovery_pb2
from proto_gen import cloud_discovery_service_pb2_grpc as discovery_grpc

app = Flask(__name__)
CORS(app)

# Database connection settings
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'ifex_offboard')
DB_USER = os.getenv('POSTGRES_USER', 'ifex')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'ifex_dev')

# Dispatcher API gRPC connection
DISPATCHER_HOST = os.getenv('DISPATCHER_HOST', 'localhost')
DISPATCHER_PORT = int(os.getenv('DISPATCHER_PORT', '50100'))

# Scheduler API gRPC connection
SCHEDULER_HOST = os.getenv('SCHEDULER_HOST', 'localhost')
SCHEDULER_PORT = int(os.getenv('SCHEDULER_PORT', '50102'))

# Discovery API gRPC connection
DISCOVERY_HOST = os.getenv('DISCOVERY_HOST', 'localhost')
DISCOVERY_PORT = int(os.getenv('DISCOVERY_PORT', '50101'))


def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def get_scheduler_channel():
    """Get a gRPC channel for the scheduler_api service (IFEX per-method stubs)"""
    return grpc.insecure_channel(f'{SCHEDULER_HOST}:{SCHEDULER_PORT}')


def get_discovery_channel():
    """Get a gRPC channel for the discovery_api service (IFEX per-method stubs)"""
    return grpc.insecure_channel(f'{DISCOVERY_HOST}:{DISCOVERY_PORT}')


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
    """Get vehicles with enrichment and service count via discovery_api gRPC"""
    # Pagination
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))

    # Filters
    fleet_filter = request.args.get('fleet', '')
    region_filter = request.args.get('region', '')
    search = request.args.get('search', '')

    try:
        # Use gRPC discovery_api to list vehicles
        channel = get_discovery_channel()
        stub = discovery_grpc.list_vehicles_serviceStub(channel)

        # Build filter - convert search to wildcard pattern
        filter_msg = discovery_pb2.list_vehicles_filter_t(
            vehicle_id_pattern=f'*{search}*' if search else '',
            fleet_id_filter=fleet_filter,
            region_filter=region_filter,
            page_size=per_page,
            page_token=str((page - 1) * per_page) if page > 1 else ''
        )

        request_msg = discovery_pb2.list_vehicles_request(filter=filter_msg)
        response = stub.list_vehicles(request_msg)

        # Convert gRPC response to JSON
        result = response.result
        vehicles = []
        for v in result.vehicles:
            vehicles.append({
                'vehicle_id': v.vehicle_id,
                'fleet_id': v.fleet_id,
                'region': v.region,
                'model': v.model,
                'year': v.year,
                'owner': v.owner,
                'is_online': v.is_online,
                'service_count': v.service_count,
                'job_count': v.job_count,
                'created_at': datetime.fromtimestamp(v.created_at_ms / 1000).isoformat() if v.created_at_ms else None,
                'updated_at': datetime.fromtimestamp(v.updated_at_ms / 1000).isoformat() if v.updated_at_ms else None
            })

        total_count = result.total_count
        total_pages = (total_count + per_page - 1) // per_page if per_page > 0 else 1

        return jsonify({
            'vehicles': vehicles,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total_count,
                'total_pages': total_pages
            }
        })

    except grpc.RpcError as e:
        return jsonify({'error': f'gRPC error: {e.details()}'}), 500


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

    # Get unique services from schema_registry with actual vehicle counts
    cur.execute("""
        SELECT
            sr.service_name,
            sr.version,
            sr.schema_hash,
            COUNT(DISTINCT vs.vehicle_id) as vehicle_count,
            sr.methods
        FROM schema_registry sr
        LEFT JOIN vehicle_schemas vs ON sr.schema_hash = vs.schema_hash
        GROUP BY sr.schema_hash, sr.service_name, sr.version, sr.methods
        ORDER BY vehicle_count DESC, sr.service_name
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
    Dispatch an RPC call to a vehicle via dispatcher_api gRPC service.

    Request body:
    {
        "vehicle_id": "VIN00000000000001",
        "service_name": "echo_service",
        "method_name": "echo",
        "parameters": {"message": "Hello"},
        "timeout_ms": 5000
    }

    Returns the full RPC response including result_json.
    """
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
    timeout_ms = data.get('timeout_ms', 10000)  # Default 10s

    try:
        # Connect to dispatcher_api gRPC service (IFEX per-method stub)
        channel = grpc.insecure_channel(f'{DISPATCHER_HOST}:{DISPATCHER_PORT}')
        stub = dispatcher_grpc.call_method_serviceStub(channel)

        # Build the RPC request (IFEX nested request pattern)
        inner_request = dispatcher_pb2.call_method_request_t(
            vehicle_id=vehicle_id,
            service_name=service_name,
            method_name=method_name,
            parameters_json=json.dumps(parameters),
            timeout_ms=timeout_ms,
            requester_id='fleet_dashboard'
        )
        grpc_request = dispatcher_pb2.call_method_request(request=inner_request)

        # Call the synchronous RPC method (blocks until response or timeout)
        grpc_timeout = (timeout_ms / 1000.0) + 5  # Add 5s buffer for network
        response = stub.call_method(grpc_request, timeout=grpc_timeout)

        channel.close()

        # Map gRPC status to string (IFEX enum values without CLOUD_ prefix)
        status_map = {
            dispatcher_pb2.RPC_SUCCESS: 'success',
            dispatcher_pb2.RPC_PENDING: 'pending',
            dispatcher_pb2.RPC_FAILED: 'failed',
            dispatcher_pb2.RPC_TIMEOUT: 'timeout',
            dispatcher_pb2.RPC_VEHICLE_OFFLINE: 'vehicle_offline',
            dispatcher_pb2.RPC_SERVICE_UNAVAILABLE: 'service_unavailable',
            dispatcher_pb2.RPC_METHOD_NOT_FOUND: 'method_not_found',
            dispatcher_pb2.RPC_INVALID_PARAMETERS: 'invalid_parameters',
            dispatcher_pb2.RPC_TRANSPORT_ERROR: 'transport_error',
            dispatcher_pb2.RPC_CANCELLED: 'cancelled',
        }
        # Response has nested .result field (IFEX pattern)
        resp_result = response.result
        status_str = status_map.get(resp_result.status, 'unknown')

        result = {
            'correlation_id': resp_result.correlation_id,
            'vehicle_id': vehicle_id,
            'service_name': service_name,
            'method_name': method_name,
            'status': status_str,
            'duration_ms': resp_result.duration_ms
        }

        if resp_result.status == dispatcher_pb2.RPC_SUCCESS:
            result['result'] = json.loads(resp_result.result_json) if resp_result.result_json else None
        else:
            result['error'] = resp_result.error_message or status_str

        return jsonify(result)

    except grpc.RpcError as e:
        return jsonify({
            'error': f'Dispatcher API error: {e.details()}',
            'status': 'grpc_error',
            'code': str(e.code())
        }), 503
    except Exception as e:
        return jsonify({
            'error': f'RPC dispatch failed: {str(e)}',
            'status': 'error'
        }), 500


# =============================================================================
# Scheduler API - Create/manage scheduled jobs on vehicles
# =============================================================================

@app.route('/api/schedule', methods=['POST'])
def schedule_job():
    """
    Schedule a job on a vehicle via scheduler_api gRPC service

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
    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    required = ['vehicle_id', 'service_name', 'method_name']
    for field in required:
        if field not in data:
            return jsonify({'error': f'Missing required field: {field}'}), 400

    vehicle_id = data['vehicle_id']

    try:
        channel = get_scheduler_channel()
        stub = scheduler_grpc.create_job_serviceStub(channel)

        # Build gRPC request (IFEX nested request pattern)
        # Convert ISO8601 strings to epoch milliseconds
        inner_request = scheduler_pb2.create_job_request_t(
            vehicle_id=vehicle_id,
            title=data.get('title', f"{data['service_name']}.{data['method_name']}"),
            service=data['service_name'],
            method=data['method_name'],
            parameters_json=json.dumps(data.get('parameters', {})),
            scheduled_time_ms=iso8601_to_epoch_ms(data.get('scheduled_time', '')),
            recurrence_rule=data.get('recurrence_rule', ''),
            created_by='fleet_dashboard'
        )
        grpc_request = scheduler_pb2.create_job_request(request=inner_request)

        response = stub.create_job(grpc_request, timeout=10.0)
        channel.close()

        # Response has nested .result field (IFEX pattern)
        resp_result = response.result
        if resp_result.success:
            return jsonify({
                'job_id': resp_result.job_id,
                'vehicle_id': vehicle_id,
                'status': 'scheduled'
            })
        else:
            return jsonify({
                'error': resp_result.error_message or 'Failed to create job',
                'vehicle_id': vehicle_id
            }), 500

    except grpc.RpcError as e:
        return jsonify({
            'error': f'Scheduler API error: {e.details()}',
            'code': str(e.code())
        }), 503
    except Exception as e:
        return jsonify({'error': f'Failed to create job: {str(e)}'}), 500


@app.route('/api/schedule/<vehicle_id>/<job_id>', methods=['DELETE'])
def delete_job(vehicle_id, job_id):
    """Delete a scheduled job on a vehicle via scheduler_api gRPC service"""
    try:
        channel = get_scheduler_channel()
        stub = scheduler_grpc.delete_job_serviceStub(channel)

        # delete_job_request has fields directly (no nested _t type)
        grpc_request = scheduler_pb2.delete_job_request(
            vehicle_id=vehicle_id,
            job_id=job_id
        )

        response = stub.delete_job(grpc_request, timeout=10.0)
        channel.close()

        # Response has nested .result (simple_response_t)
        resp_result = response.result
        if resp_result.success:
            return jsonify({
                'job_id': job_id,
                'vehicle_id': vehicle_id,
                'status': 'delete_requested'
            })
        else:
            return jsonify({
                'error': resp_result.error_message or 'Failed to delete job',
                'job_id': job_id,
                'vehicle_id': vehicle_id
            }), 500

    except grpc.RpcError as e:
        return jsonify({
            'error': f'Scheduler API error: {e.details()}',
            'code': str(e.code())
        }), 503
    except Exception as e:
        return jsonify({'error': f'Failed to delete job: {str(e)}'}), 500


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


@app.route('/api/executions')
def get_all_executions():
    """Get job executions across fleet with optional filters"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    vehicle_id = request.args.get('vehicle_id')
    status = request.args.get('status')
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('offset', 0))

    # Build query with optional filters
    query = """
        SELECT
            je.id,
            je.vehicle_id,
            je.job_id,
            je.status,
            je.executed_at_ms,
            je.duration_ms,
            je.error_message,
            je.result as result_json,
            j.title as job_title,
            j.service_name,
            j.method_name
        FROM job_executions je
        LEFT JOIN jobs j ON je.vehicle_id = j.vehicle_id AND je.job_id = j.job_id
        WHERE 1=1
    """
    params = []

    if vehicle_id:
        query += " AND je.vehicle_id = %s"
        params.append(vehicle_id)

    if status:
        query += " AND je.status = %s"
        params.append(status)

    # Get total count
    count_query = f"SELECT COUNT(*) as count FROM ({query}) subq"
    cur.execute(count_query, params)
    total = cur.fetchone()['count']

    # Get page
    query += " ORDER BY je.executed_at_ms DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    cur.execute(query, params)
    executions = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({
        'executions': executions,
        'total': total,
        'limit': limit,
        'offset': offset
    })


# =============================================================================
# Health and Status
# =============================================================================

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    status = {
        'status': 'healthy',
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

    # Check scheduler_api gRPC (IFEX healthy_service)
    try:
        channel = get_scheduler_channel()
        stub = scheduler_grpc.healthy_serviceStub(channel)
        grpc_request = scheduler_pb2.healthy_request()
        response = stub.healthy(grpc_request, timeout=2.0)
        channel.close()
        status['scheduler_api'] = 'connected' if response.is_healthy else 'unhealthy'
    except Exception as e:
        status['scheduler_api'] = f'error: {str(e)}'
        status['status'] = 'degraded'

    # Check dispatcher_api gRPC (IFEX healthy_service)
    try:
        channel = grpc.insecure_channel(f'{DISPATCHER_HOST}:{DISPATCHER_PORT}')
        stub = dispatcher_grpc.healthy_serviceStub(channel)
        grpc_request = dispatcher_pb2.healthy_request()
        response = stub.healthy(grpc_request, timeout=2.0)
        channel.close()
        status['dispatcher_api'] = 'connected' if response.is_healthy else 'unhealthy'
    except Exception as e:
        status['dispatcher_api'] = f'error: {str(e)}'
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
            sr.schema_hash,
            sr.ifex_schema,
            sr.methods,
            sr.struct_definitions,
            sr.enum_definitions,
            COUNT(DISTINCT vs.vehicle_id) as vehicle_count
        FROM schema_registry sr
        LEFT JOIN vehicle_schemas vs ON sr.schema_hash = vs.schema_hash
        WHERE sr.service_name = %s
        GROUP BY sr.schema_hash, sr.service_name, sr.version, sr.ifex_schema, sr.methods, sr.struct_definitions, sr.enum_definitions
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
        # All services grouped from schema_registry with actual vehicle counts
        cur.execute("""
            SELECT
                sr.service_name,
                sr.version,
                sr.schema_hash,
                sr.methods,
                COUNT(DISTINCT vs.vehicle_id) as vehicle_count,
                sr.first_seen_at as last_seen
            FROM schema_registry sr
            LEFT JOIN vehicle_schemas vs ON sr.schema_hash = vs.schema_hash
            GROUP BY sr.schema_hash, sr.service_name, sr.version, sr.methods, sr.first_seen_at
            ORDER BY vehicle_count DESC
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
    """Get calendar events for a vehicle via scheduler_api gRPC

    Returns sync_state matching IFEX sync_state_t enum:
    - PENDING (0): Command sent to vehicle, waiting for confirmation
    - SYNCED (1): Vehicle confirmed job exists (steady state)
    - FAILED (2): Vehicle failed to create/process job
    - OUT_OF_SYNC (3): Cloud and vehicle state have diverged
    """
    try:
        # Use gRPC scheduler_api to list jobs for this vehicle
        channel = get_scheduler_channel()
        stub = scheduler_grpc.list_jobs_serviceStub(channel)

        filter_msg = scheduler_pb2.list_jobs_filter_t(
            vehicle_id_filter=vehicle_id,
            page_size=1000  # Get all jobs for the vehicle
        )

        request_msg = scheduler_pb2.list_jobs_request(filter=filter_msg)
        response = stub.list_jobs(request_msg)

        # Convert gRPC response to JSON
        result = response.result
        calendar = []

        # Map sync_state enum to string (matches proto sync_state_t)
        # 0=SYNC_UNKNOWN, 1=SYNC_PENDING, 2=SYNC_CONFIRMED, 3=SYNC_FAILED, 4=SYNC_OUT_OF_SYNC
        sync_state_map = {0: 'unknown', 1: 'pending', 2: 'synced', 3: 'failed', 4: 'out_of_sync'}
        # Map job status enum to string
        status_map = {0: 'unknown', 1: 'scheduled', 2: 'running', 3: 'completed', 4: 'failed', 5: 'paused', 6: 'cancelled'}

        for job in result.jobs:
            calendar.append({
                'job_id': job.job_id,
                'title': job.title,
                'service_name': job.service,
                'method_name': job.method,
                'parameters': json.loads(job.parameters_json) if job.parameters_json else {},
                'scheduled_time': epoch_ms_to_iso8601(job.scheduled_time_ms),
                'scheduled_time_ms': job.scheduled_time_ms,  # Also include raw ms for JavaScript
                'recurrence_rule': job.recurrence_rule,
                'next_run_time': epoch_ms_to_iso8601(job.next_run_time_ms),
                'next_run_time_ms': job.next_run_time_ms,
                'sync_state': sync_state_map.get(job.sync_state, 'unknown'),
                'job_status': status_map.get(job.status, 'unknown'),
                'created_by': job.created_by,
                'created_at': epoch_ms_to_iso8601(job.created_at_ms),
                'created_at_ms': job.created_at_ms,
                'synced_at': epoch_ms_to_iso8601(job.synced_at_ms) if job.synced_at_ms else None,
                'synced_at_ms': job.synced_at_ms
            })

        # Sort by scheduled_time
        calendar.sort(key=lambda x: x['scheduled_time'] or '')

        # Get vehicle online status for UI compatibility
        vehicle_online = False
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT is_online FROM vehicles WHERE vehicle_id = %s", (vehicle_id,))
            row = cur.fetchone()
            if row:
                vehicle_online = row['is_online']
            cur.close()
            conn.close()
        except:
            pass

        return jsonify({
            'vehicle_id': vehicle_id,
            'vehicle_online': vehicle_online,
            'calendar': calendar,
            'total_count': result.total_count
        })

    except grpc.RpcError as e:
        return jsonify({'error': f'gRPC error: {e.details()}'}), 500


@app.route('/api/calendar/<vehicle_id>', methods=['POST'])
def create_calendar_entry(vehicle_id):
    """
    Create a calendar entry for a vehicle via scheduler_api gRPC service

    The scheduler_api handles all routing (online/offline vehicle handling).
    We also store in offboard_calendar for dashboard tracking.

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
    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    required = ['service_name', 'method_name', 'scheduled_time']
    for field in required:
        if not data.get(field):
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

    scheduled_time = data.get('scheduled_time')
    sync_status = 'pending'
    sync_error = None
    job_id = None

    # Send to scheduler_api via gRPC (IFEX per-method stub)
    try:
        channel = get_scheduler_channel()
        stub = scheduler_grpc.create_job_serviceStub(channel)

        # Convert ISO8601 strings to epoch milliseconds
        inner_request = scheduler_pb2.create_job_request_t(
            vehicle_id=vehicle_id,
            title=data.get('title', f"{data['service_name']}.{data['method_name']}"),
            service=data['service_name'],
            method=data['method_name'],
            parameters_json=json.dumps(data.get('parameters', {})),
            scheduled_time_ms=iso8601_to_epoch_ms(scheduled_time),
            recurrence_rule=data.get('recurrence_rule', ''),
            end_time_ms=iso8601_to_epoch_ms(data.get('end_time', '')),
            created_by=data.get('created_by', 'fleet_dashboard')
        )
        grpc_request = scheduler_pb2.create_job_request(request=inner_request)

        response = stub.create_job(grpc_request, timeout=10.0)
        channel.close()

        resp_result = response.result
        if resp_result.success:
            job_id = resp_result.job_id
            sync_status = 'synced'
        else:
            sync_error = resp_result.error_message or 'Failed to create job'

    except grpc.RpcError as e:
        sync_error = f'Scheduler API error: {e.details()}'
    except Exception as e:
        sync_error = str(e)

    # Store in offboard_calendar for dashboard tracking (with job_id from scheduler)
    if job_id:
        try:
            cur.execute("""
                INSERT INTO offboard_calendar
                (vehicle_id, job_id, title, service_name, method_name, parameters,
                 scheduled_time, recurrence_rule, end_time,
                 wake_policy, sleep_policy, wake_lead_time_s,
                 sync_status, synced_at, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (job_id) DO UPDATE SET sync_status = 'synced', synced_at = NOW()
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
                sync_status,
                data.get('created_by', 'fleet_dashboard')
            ))
            conn.commit()
        except Exception as e:
            # Log but don't fail - gRPC call succeeded
            print(f"Warning: Failed to store in offboard_calendar: {e}")

    cur.close()
    conn.close()

    if sync_error and not job_id:
        return jsonify({
            'error': sync_error,
            'vehicle_id': vehicle_id,
            'vehicle_online': vehicle['is_online']
        }), 503

    return jsonify({
        'job_id': job_id,
        'vehicle_id': vehicle_id,
        'vehicle_online': vehicle['is_online'],
        'sync_status': sync_status,
        'sync_error': sync_error,
        'scheduled_time': scheduled_time
    })


@app.route('/api/calendar/<vehicle_id>/command', methods=['POST'])
def send_calendar_command(vehicle_id):
    """
    Send a scheduler command for a job via scheduler_api gRPC service

    Supported command types:
    - update_job: Update job parameters, scheduled_time, recurrence
    - delete_job: Delete the job
    - pause_job: Pause the job
    - resume_job: Resume a paused job
    - trigger_job: Trigger immediate execution

    Request body:
    {
        "command_type": "update_job|delete_job|pause_job|resume_job|trigger_job",
        "job_id": "uuid",
        "scheduled_time": "2026-01-15T10:00:00Z",  (for update_job)
        "parameters": {...},  (for update_job)
        "recurrence_rule": "FREQ=DAILY"  (for update_job)
    }
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    command_type = data.get('command_type')
    job_id = data.get('job_id')

    if not command_type:
        return jsonify({'error': 'Missing required field: command_type'}), 400
    if not job_id:
        return jsonify({'error': 'Missing required field: job_id'}), 400

    command_type_normalized = command_type.lower()
    valid_commands = ['update_job', 'delete_job', 'pause_job', 'resume_job', 'trigger_job']

    if command_type_normalized not in valid_commands:
        return jsonify({'error': f'Unknown command_type: {command_type}'}), 400

    # Send to scheduler_api via gRPC (IFEX per-method stubs)
    try:
        channel = get_scheduler_channel()
        response = None

        if command_type_normalized == 'update_job':
            stub = scheduler_grpc.update_job_serviceStub(channel)
            # Convert ISO8601 strings to epoch milliseconds (0 = no change)
            inner_request = scheduler_pb2.update_job_request_t(
                vehicle_id=vehicle_id,
                job_id=job_id,
                title=data.get('title') or '',
                scheduled_time_ms=iso8601_to_epoch_ms(data.get('scheduled_time', '')),
                recurrence_rule=data.get('recurrence_rule') or '',
                parameters_json=json.dumps(data['parameters']) if 'parameters' in data else (data.get('parameters_json') or ''),
                end_time_ms=iso8601_to_epoch_ms(data.get('end_time', ''))
            )
            grpc_request = scheduler_pb2.update_job_request(request=inner_request)
            response = stub.update_job(grpc_request, timeout=10.0)

        elif command_type_normalized == 'delete_job':
            stub = scheduler_grpc.delete_job_serviceStub(channel)
            grpc_request = scheduler_pb2.delete_job_request(
                vehicle_id=vehicle_id,
                job_id=job_id
            )
            response = stub.delete_job(grpc_request, timeout=10.0)

        elif command_type_normalized == 'pause_job':
            stub = scheduler_grpc.pause_job_serviceStub(channel)
            grpc_request = scheduler_pb2.pause_job_request(
                vehicle_id=vehicle_id,
                job_id=job_id
            )
            response = stub.pause_job(grpc_request, timeout=10.0)

        elif command_type_normalized == 'resume_job':
            stub = scheduler_grpc.resume_job_serviceStub(channel)
            grpc_request = scheduler_pb2.resume_job_request(
                vehicle_id=vehicle_id,
                job_id=job_id
            )
            response = stub.resume_job(grpc_request, timeout=10.0)

        elif command_type_normalized == 'trigger_job':
            stub = scheduler_grpc.trigger_job_serviceStub(channel)
            grpc_request = scheduler_pb2.trigger_job_request(
                vehicle_id=vehicle_id,
                job_id=job_id
            )
            response = stub.trigger_job(grpc_request, timeout=10.0)

        channel.close()

        # All responses have nested .result (simple_response_t)
        resp_result = response.result
        if not resp_result.success:
            return jsonify({
                'error': resp_result.error_message or f'Failed to {command_type}',
                'job_id': job_id,
                'vehicle_id': vehicle_id
            }), 500

    except grpc.RpcError as e:
        return jsonify({
            'error': f'Scheduler API error: {e.details()}',
            'code': str(e.code())
        }), 503
    except Exception as e:
        return jsonify({'error': f'Failed to send command: {str(e)}'}), 500

    # Update offboard_calendar if applicable
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        if command_type_normalized == 'update_job':
            # Update offboard_calendar with new values
            updates = []
            params = []
            if 'scheduled_time' in data:
                updates.append("scheduled_time = %s")
                params.append(data['scheduled_time'])
            if 'parameters' in data:
                updates.append("parameters = %s")
                params.append(json.dumps(data['parameters']))
            elif 'parameters_json' in data:
                updates.append("parameters = %s")
                params.append(data['parameters_json'])
            if 'recurrence_rule' in data:
                updates.append("recurrence_rule = %s")
                params.append(data['recurrence_rule'])
            if 'title' in data:
                updates.append("title = %s")
                params.append(data['title'])

            if updates:
                updates.append("updated_at = NOW()")
                params.extend([vehicle_id, job_id])
                cur.execute(f"""
                    UPDATE offboard_calendar
                    SET {', '.join(updates)}
                    WHERE vehicle_id = %s AND job_id = %s
                """, params)
                conn.commit()

        elif command_type_normalized == 'delete_job':
            # Remove from offboard_calendar
            cur.execute("""
                DELETE FROM offboard_calendar
                WHERE vehicle_id = %s AND job_id = %s
            """, (vehicle_id, job_id))
            conn.commit()

    except Exception as e:
        conn.rollback()
        # Log but don't fail - gRPC command was sent
        print(f"Warning: Failed to update offboard_calendar: {e}")

    finally:
        cur.close()
        conn.close()

    return jsonify({
        'job_id': job_id,
        'vehicle_id': vehicle_id,
        'command_type': command_type,
        'status': 'sent'
    })


@app.route('/api/calendar/<vehicle_id>/<job_id>', methods=['DELETE'])
def delete_calendar_entry(vehicle_id, job_id):
    """Delete a calendar entry via scheduler_api gRPC service"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Check if it's in offboard_calendar (not yet synced)
    cur.execute("""
        SELECT id, sync_status FROM offboard_calendar
        WHERE vehicle_id = %s AND job_id = %s
    """, (vehicle_id, job_id))
    offboard_entry = cur.fetchone()

    if offboard_entry and offboard_entry['sync_status'] == 'pending':
        # Just delete from offboard_calendar (wasn't synced yet)
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

    # Send delete command via scheduler_api gRPC (IFEX per-method stub)
    try:
        channel = get_scheduler_channel()
        stub = scheduler_grpc.delete_job_serviceStub(channel)

        grpc_request = scheduler_pb2.delete_job_request(
            vehicle_id=vehicle_id,
            job_id=job_id
        )

        response = stub.delete_job(grpc_request, timeout=10.0)
        channel.close()

        resp_result = response.result
        if resp_result.success:
            return jsonify({
                'job_id': job_id,
                'vehicle_id': vehicle_id,
                'status': 'delete_sent',
                'vehicle_online': vehicle['is_online']
            })
        else:
            return jsonify({
                'error': resp_result.error_message or 'Failed to delete job',
                'job_id': job_id,
                'vehicle_id': vehicle_id
            }), 500

    except grpc.RpcError as e:
        return jsonify({
            'error': f'Scheduler API error: {e.details()}',
            'code': str(e.code())
        }), 503
    except Exception as e:
        return jsonify({'error': f'Failed to send command: {str(e)}'}), 500


if __name__ == '__main__':
    port = int(os.getenv('DASHBOARD_PORT', 5000))
    print(f"Starting IFEX Fleet Dashboard API on port {port}")
    print(f"Database: {DB_NAME}@{DB_HOST}:{DB_PORT}")
    print(f"Dispatcher API: {DISPATCHER_HOST}:{DISPATCHER_PORT}")
    print(f"Scheduler API: {SCHEDULER_HOST}:{SCHEDULER_PORT}")

    print(f"Open http://localhost:{port}/ in your browser")
    app.run(host='0.0.0.0', port=port, debug=True)
