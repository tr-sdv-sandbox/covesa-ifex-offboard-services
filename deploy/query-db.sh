#!/bin/bash
# Quick database query utility for IFEX Offboard test environment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PSQL_CMD="docker-compose exec -T postgres psql -U ifex -d ifex_offboard"

case "${1:-stats}" in
    stats)
        echo "=== Database Statistics ==="
        $PSQL_CMD -c "
            SELECT 'vehicles' as table_name, COUNT(*) as count FROM vehicles
            UNION ALL SELECT 'vehicle_enrichment', COUNT(*) FROM vehicle_enrichment
            UNION ALL SELECT 'services', COUNT(*) FROM services
            UNION ALL SELECT 'jobs', COUNT(*) FROM jobs
            UNION ALL SELECT 'job_executions', COUNT(*) FROM job_executions
            UNION ALL SELECT 'rpc_requests', COUNT(*) FROM rpc_requests
            UNION ALL SELECT 'sync_state', COUNT(*) FROM sync_state
            ORDER BY table_name;
        "
        ;;

    vehicles)
        echo "=== Sample Vehicles ==="
        $PSQL_CMD -c "
            SELECT vehicle_id, is_online,
                   metadata->>'fleet_id' as fleet,
                   metadata->>'region' as region,
                   metadata->>'model' as model,
                   last_seen_at
            FROM vehicles
            ORDER BY last_seen_at DESC
            LIMIT 20;
        "
        ;;

    services)
        echo "=== Services by Type ==="
        $PSQL_CMD -c "
            SELECT service_name, transport_type,
                   COUNT(*) as count,
                   SUM(CASE WHEN status = 'available' THEN 1 ELSE 0 END) as available
            FROM services
            GROUP BY service_name, transport_type
            ORDER BY count DESC;
        "
        ;;

    jobs)
        echo "=== Jobs by Status ==="
        $PSQL_CMD -c "
            SELECT status, COUNT(*) as count,
                   COUNT(DISTINCT vehicle_id) as vehicles
            FROM jobs
            GROUP BY status
            ORDER BY count DESC;
        "
        ;;

    fleets)
        echo "=== Fleet Summary ==="
        $PSQL_CMD -c "
            SELECT e.fleet_id as fleet,
                   e.region,
                   COUNT(*) as vehicles,
                   SUM(CASE WHEN v.is_online THEN 1 ELSE 0 END) as online
            FROM vehicle_enrichment e
            JOIN vehicles v ON e.vehicle_id = v.vehicle_id
            GROUP BY e.fleet_id, e.region
            ORDER BY fleet, region;
        "
        ;;

    enrichment)
        echo "=== Sample Enrichment Data ==="
        $PSQL_CMD -c "
            SELECT vehicle_id, fleet_id, region, model, year, owner,
                   updated_at
            FROM vehicle_enrichment
            ORDER BY updated_at DESC
            LIMIT 20;
        "
        ;;

    rpc)
        echo "=== Recent RPC Requests ==="
        $PSQL_CMD -c "
            SELECT correlation_id,
                   SUBSTRING(vehicle_id, 1, 15) as vehicle,
                   service_name,
                   method_name,
                   response_status,
                   duration_ms
            FROM rpc_requests
            ORDER BY created_at DESC
            LIMIT 20;
        "
        ;;

    sql)
        shift
        if [ -z "$1" ]; then
            echo "Usage: $0 sql 'SELECT * FROM ...'"
            exit 1
        fi
        $PSQL_CMD -c "$*"
        ;;

    psql)
        docker-compose exec postgres psql -U ifex -d ifex_offboard
        ;;

    *)
        echo "Usage: $0 <command>"
        echo ""
        echo "Commands:"
        echo "  stats       Show table row counts (default)"
        echo "  vehicles    List sample vehicles"
        echo "  enrichment  Show sample enrichment data"
        echo "  services    Show services by type"
        echo "  jobs        Show jobs by status"
        echo "  fleets      Show fleet summary"
        echo "  rpc         Show recent RPC requests"
        echo "  sql 'Q'     Run custom SQL query"
        echo "  psql        Open interactive psql session"
        exit 1
        ;;
esac
