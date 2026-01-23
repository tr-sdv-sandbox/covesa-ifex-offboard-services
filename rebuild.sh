#!/bin/bash
# Rebuild C++ binaries and Docker dashboard container

set -e

mkdir -p build

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== Rebuilding C++ binaries ==="
cd build
cmake ..
make -j$(nproc)
cd ..

echo ""
echo "=== Rebuilding dashboard Docker container ==="
cd deploy
docker-compose build --no-cache dashboard
cd ..

echo ""
echo "=== Done! ==="
echo "Restart your simulation to pick up changes."
