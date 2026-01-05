#!/bin/bash
# Teardown script for IFEX Offboard Services test environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# Parse arguments
REMOVE_VOLUMES=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --volumes|-v)
            REMOVE_VOLUMES=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --volumes, -v  Also remove data volumes (destructive)"
            echo "  --help         Show this help message"
            exit 0
            ;;
        *)
            log_warn "Unknown option: $1"
            shift
            ;;
    esac
done

log_info "Stopping IFEX Offboard test environment..."

if [ "$REMOVE_VOLUMES" = true ]; then
    log_warn "Removing containers AND volumes (all data will be lost)"
    docker-compose --profile ui down -v
else
    docker-compose --profile ui down
    log_info "Containers stopped. Data volumes preserved."
    log_info "Use --volumes to also remove data."
fi

log_info "Done!"
