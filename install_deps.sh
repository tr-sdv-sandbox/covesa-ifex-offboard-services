#!/bin/bash
# Install dependencies for IFEX Offboard Services (Ubuntu 24.04)

set -e

echo "Installing IFEX Offboard Services dependencies..."

sudo apt-get update --allow-releaseinfo-change

sudo apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libprotobuf-dev \
    protobuf-compiler \
    libgrpc++-dev \
    protobuf-compiler-grpc \
    libgoogle-glog-dev \
    libgflags-dev \
    libpq-dev \
    libssl-dev \
    librdkafka-dev \
    libpaho-mqtt-dev \
    libpaho-mqttpp-dev

echo ""
echo "Dependencies installed. Next:"
echo "  cd docker && docker compose up -d"
echo "  mkdir build && cd build && cmake .. && make -j"
