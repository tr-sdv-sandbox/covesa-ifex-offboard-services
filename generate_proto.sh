#!/bin/bash

# Script to generate proto files from IFEX YAML definitions
# Uses the official IFEX Docker-based tool

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Template directory from ifex-core (shared templates)
IFEX_CORE_DIR="${SCRIPT_DIR}/../covesa-ifex-core"
TEMPLATE_DIR="${IFEX_CORE_DIR}/templates"

# Check if templates exist
if [ ! -d "${TEMPLATE_DIR}" ]; then
    echo "Error: Template directory not found at ${TEMPLATE_DIR}"
    echo "Make sure covesa-ifex-core is checked out at the same level."
    exit 1
fi

# Check if ifex Docker image is available
if ! docker images | grep -q "ifex-tools"; then
    echo "Error: ifex-tools Docker image not found."
    echo "Please run install_deps.sh in covesa-ifex-core first."
    exit 1
fi

echo "Generating proto files from IFEX YAML definitions..."
echo ""

# Function to process IFEX file
process_ifex_file() {
    local yaml_file="$1"
    local output_dir="$2"
    local output_name="$3"

    local proto_file="${output_dir}/${output_name}.proto"

    echo "Processing ${yaml_file}..."

    # Use the IFEX tool via Docker with template override
    docker run --rm \
        -v "${SCRIPT_DIR}:/workspace" \
        -v "${TEMPLATE_DIR}:/templates:ro" \
        -w /workspace \
        ifex-tools:latest \
        ifexgen -d /templates/protobuf "${yaml_file}" > "${proto_file}"

    echo "  Generated ${proto_file}"
}

# =============================================================================
# Generate protos for offboard-platform library
# =============================================================================

OFFBOARD_PLATFORM_DIR="libs/offboard-platform"
if [ -d "${SCRIPT_DIR}/${OFFBOARD_PLATFORM_DIR}/ifex" ]; then
    echo ""
    echo "=== offboard-platform ==="

    for yaml_file in "${SCRIPT_DIR}/${OFFBOARD_PLATFORM_DIR}"/ifex/*.yml; do
        if [ -f "$yaml_file" ]; then
            basename=$(basename "$yaml_file" .yml)
            process_ifex_file \
                "${OFFBOARD_PLATFORM_DIR}/ifex/${basename}.yml" \
                "${OFFBOARD_PLATFORM_DIR}" \
                "${basename}"
        fi
    done
fi

# =============================================================================
# Generate Python protobuf bindings for dashboard
# =============================================================================

DASHBOARD_PROTO_DIR="${SCRIPT_DIR}/dashboard/proto_gen"
if [ -d "${DASHBOARD_PROTO_DIR}" ]; then
    echo ""
    echo "=== Python protobuf bindings for dashboard ==="

    # Create/activate venv and install grpcio-tools
    VENV_DIR="${SCRIPT_DIR}/.venv"
    if [ ! -d "${VENV_DIR}" ]; then
        echo "  Creating Python virtual environment..."
        python3 -m venv "${VENV_DIR}"
    fi

    # Install grpcio-tools in venv if needed
    if ! "${VENV_DIR}/bin/python3" -c "import grpc_tools.protoc" 2>/dev/null; then
        echo "  Installing grpcio-tools in venv..."
        "${VENV_DIR}/bin/pip" install --quiet grpcio-tools
    fi

    for proto_file in "${SCRIPT_DIR}/${OFFBOARD_PLATFORM_DIR}"/*.proto; do
        if [ -f "$proto_file" ]; then
            basename=$(basename "$proto_file" .proto)
            echo "  Generating Python bindings for ${basename}..."
            "${VENV_DIR}/bin/python3" -m grpc_tools.protoc \
                -I "${SCRIPT_DIR}/${OFFBOARD_PLATFORM_DIR}" \
                --python_out="${DASHBOARD_PROTO_DIR}" \
                --grpc_python_out="${DASHBOARD_PROTO_DIR}" \
                "$proto_file"
        fi
    done

    # Fix imports in generated files (grpc_tools generates absolute imports)
    echo "  Fixing Python imports..."
    for py_file in "${DASHBOARD_PROTO_DIR}"/*_pb2_grpc.py; do
        if [ -f "$py_file" ]; then
            # Replace "import xxx_pb2" with "from . import xxx_pb2"
            sed -i 's/^import \([a-z_]*_pb2\)/from . import \1/' "$py_file"
        fi
    done

    echo "  Python bindings generated in ${DASHBOARD_PROTO_DIR}"
fi

echo ""
echo "Proto generation complete!"
echo ""
echo "Next steps:"
echo "1. Run cmake to regenerate C++ from proto files"
echo "2. Rebuild the project"
