#!/bin/bash
# Stop IFEX Simulation Environment
# Usage: ./stop-simulation.sh [--keep-infra]
#
# Options:
#   --keep-infra    Keep infrastructure running, only stop vehicles

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

KEEP_INFRA=false
if [ "$1" = "--keep-infra" ]; then
    KEEP_INFRA=true
fi

# Colors
GREEN='\033[0;32m'
NC='\033[0m'
log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }

log_info "Stopping IFEX Simulation..."

# Stop vehicle containers
VEHICLE_CONTAINERS=$(docker ps -aq --filter "name=ifex-vehicle-" 2>/dev/null || true)
if [ -n "$VEHICLE_CONTAINERS" ]; then
    VEHICLE_COUNT=$(echo "$VEHICLE_CONTAINERS" | wc -w)
    docker rm -f $VEHICLE_CONTAINERS >/dev/null 2>&1 || true
    log_info "Stopped $VEHICLE_COUNT vehicle containers"
else
    log_info "No vehicle containers running"
fi

# Stop infrastructure if requested
if [ "$KEEP_INFRA" = false ]; then
    log_info "Stopping infrastructure..."
    ./stop-infra.sh
else
    log_info "Keeping infrastructure running (--keep-infra)"
fi

# Clean up network
docker network rm ifex-simulation 2>/dev/null || true

log_info "Simulation stopped"
