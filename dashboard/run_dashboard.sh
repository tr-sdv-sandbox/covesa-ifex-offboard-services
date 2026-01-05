#!/bin/bash
# Run IFEX Fleet Dashboard
# Usage: ./run_dashboard.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
else
    source venv/bin/activate
fi

# Set defaults
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_DB="${POSTGRES_DB:-ifex_offboard}"
export POSTGRES_USER="${POSTGRES_USER:-ifex}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-ifex_dev}"
export DASHBOARD_PORT="${DASHBOARD_PORT:-5000}"

echo "================================================"
echo "  IFEX Fleet Dashboard"
echo "================================================"
echo ""
echo "Database: $POSTGRES_DB @ $POSTGRES_HOST:$POSTGRES_PORT"
echo "Dashboard: http://localhost:$DASHBOARD_PORT/"
echo ""
echo "Press Ctrl+C to stop"
echo ""

python fleet_api.py
