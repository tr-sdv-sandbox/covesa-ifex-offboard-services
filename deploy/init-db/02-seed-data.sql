-- Seed data for IFEX Offboard Services test environment
-- Generates 1000 test vehicles with services, jobs, and sync state

-- Configuration
\set num_vehicles 1000
\set services_per_vehicle 5
\set jobs_per_vehicle 3

-- =============================================================================
-- Helper function to generate realistic VINs
-- =============================================================================
CREATE OR REPLACE FUNCTION generate_vin(seq INTEGER) RETURNS VARCHAR AS $$
DECLARE
    -- VIN format: WMI (3) + VDS (6) + VIS (8)
    -- Using TRATON-style prefixes
    wmi_options TEXT[] := ARRAY['WDB', 'WVW', 'WBA', 'MAN', 'SCA', 'DAF'];
    wmi TEXT;
    vds TEXT;
    vis TEXT;
BEGIN
    wmi := wmi_options[1 + (seq % array_length(wmi_options, 1))];
    vds := LPAD(TO_HEX(seq * 7 % 16777215), 6, '0');
    vis := LPAD(seq::TEXT, 8, '0');
    RETURN UPPER(wmi || vds || vis);
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Insert 1000 vehicles
-- =============================================================================
-- Note: is_online is false for all seeded vehicles
-- Real online status is set by MQTT LWT and status messages at runtime
INSERT INTO vehicles (vehicle_id, first_seen_at, last_seen_at, is_online)
SELECT
    generate_vin(n) as vehicle_id,
    NOW() - (random() * interval '90 days') as first_seen_at,
    NULL as last_seen_at,  -- Never seen until they actually connect
    false as is_online
FROM generate_series(1, 1000) as n;

-- =============================================================================
-- Insert enrichment data for all vehicles
-- This is the source of truth for fleet assignment, region, etc.
-- =============================================================================
INSERT INTO vehicle_enrichment (vehicle_id, fleet_id, region, model, year, owner, tags)
SELECT
    generate_vin(n) as vehicle_id,
    'fleet-' || LPAD((1 + (n % 10))::TEXT, 3, '0') as fleet_id,
    (ARRAY['eu-west', 'eu-central', 'eu-north', 'us-east', 'us-west', 'apac'])[1 + (n % 6)] as region,
    (ARRAY['TGX', 'TGS', 'TGM', 'TGL', 'MAN Lion', 'Scania R', 'Scania S'])[1 + (n % 7)] as model,
    2020 + (n % 5) as year,
    'Fleet Operator ' || (1 + (n % 10)) as owner,
    CASE WHEN n % 20 = 0 THEN '["priority", "monitored"]'::jsonb
         WHEN n % 10 = 0 THEN '["monitored"]'::jsonb
         ELSE '[]'::jsonb
    END as tags
FROM generate_series(1, 1000) as n;

-- =============================================================================
-- Insert services for each vehicle (5 per vehicle = 5000 total)
-- =============================================================================
INSERT INTO services (
    vehicle_id, registration_id, service_name, version, description,
    endpoint_address, transport_type, status, last_heartbeat_ms, namespaces
)
SELECT
    v.vehicle_id,
    'svc-' || v.vehicle_id || '-' || s.idx as registration_id,
    (ARRAY[
        'climate.hvac',
        'infotainment.media',
        'navigation.routing',
        'diagnostics.obd',
        'telematics.fleet',
        'adas.collision',
        'powertrain.engine',
        'body.doors',
        'chassis.suspension',
        'lighting.exterior'
    ])[1 + ((row_number() OVER (PARTITION BY v.vehicle_id ORDER BY s.idx) - 1) % 10)] as service_name,
    '1.' || (s.idx % 5) || '.0' as version,
    'Auto-generated test service ' || s.idx as description,
    'unix:///var/run/ifex/' || s.idx || '.sock' as endpoint_address,
    (ARRAY['grpc', 'someip', 'dbus', 'http_rest'])[1 + (s.idx % 4)] as transport_type,
    (ARRAY['available', 'available', 'available', 'unavailable', 'starting'])[1 + floor(random() * 5)::int] as status,
    EXTRACT(EPOCH FROM NOW() - (random() * interval '1 hour'))::BIGINT * 1000 as last_heartbeat_ms,
    jsonb_build_array(
        jsonb_build_object(
            'name', 'default',
            'methods', jsonb_build_array(
                jsonb_build_object('name', 'get_status', 'description', 'Get current status'),
                jsonb_build_object('name', 'set_config', 'description', 'Set configuration')
            )
        )
    ) as namespaces
FROM vehicles v
CROSS JOIN generate_series(1, 5) as s(idx);

-- =============================================================================
-- Insert jobs for each vehicle (3 per vehicle = 3000 total)
-- =============================================================================
INSERT INTO jobs (
    vehicle_id, job_id, title, service_name, method_name, parameters,
    scheduled_time, recurrence_rule, next_run_time, status,
    created_at_ms, updated_at_ms
)
SELECT
    v.vehicle_id,
    'job-' || v.vehicle_id || '-' || j.idx as job_id,
    (ARRAY[
        'Daily diagnostics check',
        'Hourly telemetry upload',
        'Weekly maintenance report',
        'Daily fuel economy report',
        'Periodic software check',
        'Driver behavior analysis'
    ])[1 + ((row_number() OVER (PARTITION BY v.vehicle_id ORDER BY j.idx) - 1) % 6)] as title,
    (ARRAY['diagnostics.obd', 'telematics.fleet', 'powertrain.engine'])[1 + (j.idx % 3)] as service_name,
    (ARRAY['run_diagnostics', 'upload_telemetry', 'generate_report'])[1 + (j.idx % 3)] as method_name,
    jsonb_build_object(
        'detail_level', (ARRAY['basic', 'standard', 'detailed'])[1 + (j.idx % 3)],
        'include_history', j.idx % 2 = 0
    ) as parameters,
    TO_CHAR(NOW() + (j.idx || ' hours')::interval, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as scheduled_time,
    CASE j.idx % 3
        WHEN 0 THEN 'FREQ=DAILY;BYHOUR=6'
        WHEN 1 THEN 'FREQ=HOURLY'
        ELSE 'FREQ=WEEKLY;BYDAY=MO'
    END as recurrence_rule,
    TO_CHAR(NOW() + (j.idx || ' hours')::interval, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as next_run_time,
    (ARRAY['pending', 'pending', 'pending', 'running', 'completed'])[1 + floor(random() * 5)::int] as status,
    EXTRACT(EPOCH FROM NOW() - (random() * interval '30 days'))::BIGINT * 1000 as created_at_ms,
    EXTRACT(EPOCH FROM NOW() - (random() * interval '1 day'))::BIGINT * 1000 as updated_at_ms
FROM vehicles v
CROSS JOIN generate_series(1, 3) as j(idx);

-- =============================================================================
-- Insert some job execution history (10 executions per vehicle = 10000 total)
-- =============================================================================
INSERT INTO job_executions (
    vehicle_id, job_id, status, executed_at_ms, duration_ms,
    result, error_message, next_run_time
)
SELECT
    v.vehicle_id,
    'job-' || v.vehicle_id || '-' || (1 + (e.idx % 3)) as job_id,
    (ARRAY['completed', 'completed', 'completed', 'completed', 'failed'])[1 + floor(random() * 5)::int] as status,
    EXTRACT(EPOCH FROM NOW() - ((e.idx * 6) || ' hours')::interval)::BIGINT * 1000 as executed_at_ms,
    (100 + floor(random() * 5000))::INTEGER as duration_ms,
    CASE WHEN random() > 0.2
        THEN '{"status": "ok", "items_processed": ' || floor(random() * 100)::int || '}'
        ELSE NULL
    END as result,
    CASE WHEN random() > 0.8
        THEN 'Service temporarily unavailable'
        ELSE NULL
    END as error_message,
    TO_CHAR(NOW() + ((e.idx + 1) * 6 || ' hours')::interval, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') as next_run_time
FROM vehicles v
CROSS JOIN generate_series(1, 10) as e(idx);

-- =============================================================================
-- Insert sync state for each vehicle
-- =============================================================================
INSERT INTO sync_state (
    vehicle_id,
    discovery_sequence, discovery_checksum, discovery_last_sync,
    scheduler_sequence, scheduler_checksum, scheduler_last_sync
)
SELECT
    vehicle_id,
    floor(random() * 10000)::BIGINT as discovery_sequence,
    floor(random() * 2147483647)::INTEGER as discovery_checksum,
    NOW() - (random() * interval '1 hour') as discovery_last_sync,
    floor(random() * 5000)::BIGINT as scheduler_sequence,
    floor(random() * 2147483647)::INTEGER as scheduler_checksum,
    NOW() - (random() * interval '2 hours') as scheduler_last_sync
FROM vehicles;

-- =============================================================================
-- Summary statistics
-- =============================================================================
DO $$
DECLARE
    v_count INTEGER;
    s_count INTEGER;
    j_count INTEGER;
    e_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM vehicles;
    SELECT COUNT(*) INTO s_count FROM services;
    SELECT COUNT(*) INTO j_count FROM jobs;
    SELECT COUNT(*) INTO e_count FROM job_executions;

    RAISE NOTICE '=== Seed Data Summary ===';
    RAISE NOTICE 'Vehicles:        %', v_count;
    RAISE NOTICE 'Services:        %', s_count;
    RAISE NOTICE 'Jobs:            %', j_count;
    RAISE NOTICE 'Job Executions:  %', e_count;
    RAISE NOTICE '=========================';
END $$;

-- Cleanup helper function
DROP FUNCTION IF EXISTS generate_vin(INTEGER);
