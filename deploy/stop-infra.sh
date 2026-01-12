#!/bin/bash
# Stop IFEX Offboard Infrastructure
# Usage: ./stop-infra.sh [--keep-docker]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

KEEP_DOCKER=false
if [ "$1" = "--keep-docker" ]; then
    KEEP_DOCKER=true
fi

# Colors
GREEN='\033[0;32m'
NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

log_info "Stopping IFEX Offboard Infrastructure..."

# Stop services by PID
for service in mqtt_kafka_bridge discovery_mirror scheduler_mirror enrichment_exporter dispatcher_api discovery_api scheduler_api fleet_dashboard; do
    pidfile="/tmp/ifex-pids/${service}.pid"
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            log_info "Stopped $service (PID: $pid)"
        fi
        rm -f "$pidfile"
    fi
done

# Also pkill by name in case PIDs are stale
pkill -f "mqtt_kafka_bridge" 2>/dev/null || true
pkill -f "discovery_mirror" 2>/dev/null || true
pkill -f "scheduler_mirror" 2>/dev/null || true
pkill -f "enrichment_exporter" 2>/dev/null || true
pkill -f "dispatcher_api" 2>/dev/null || true
pkill -f "discovery_api" 2>/dev/null || true
pkill -f "scheduler_api" 2>/dev/null || true
pkill -f "fleet_api.py" 2>/dev/null || true

# Stop Docker if requested
if [ "$KEEP_DOCKER" = false ]; then
    log_info "Stopping Docker containers..."
    docker-compose down
else
    log_info "Keeping Docker containers running (--keep-docker)"
fi

log_info "Infrastructure stopped"
