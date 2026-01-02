#!/bin/bash
# Stop development infrastructure

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Stopping IFEX Offboard development infrastructure..."

cd "$PROJECT_DIR/docker"

docker compose down

echo "Infrastructure stopped."
