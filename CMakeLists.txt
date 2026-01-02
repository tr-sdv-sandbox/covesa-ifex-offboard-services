cmake_minimum_required(VERSION 3.16)
project(covesa-ifex-offboard-services VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Options
option(BUILD_TESTS "Build tests" OFF)

# Find required packages
find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG QUIET)
find_package(glog REQUIRED)
find_package(gflags REQUIRED)

# Find librdkafka
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)

# Find libpq (PostgreSQL)
pkg_check_modules(LIBPQ REQUIRED libpq)

# Find Paho MQTT C++
find_library(PAHO_MQTT_CPP paho-mqttpp3 REQUIRED)
find_library(PAHO_MQTT_C paho-mqtt3as REQUIRED)

# gRPC plugin for protobuf generation
if(gRPC_FOUND)
    set(GRPC_CPP_PLUGIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

# Proto generation output directory
set(PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_DIR})

# Collect proto files
file(GLOB PROTO_FILES ${CMAKE_CURRENT_SOURCE_DIR}/proto/*.proto)

# Generate protobuf and gRPC sources
foreach(PROTO_FILE ${PROTO_FILES})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)

    set(PROTO_SRC ${PROTO_GEN_DIR}/${PROTO_NAME}.pb.cc)
    set(PROTO_HDR ${PROTO_GEN_DIR}/${PROTO_NAME}.pb.h)
    set(GRPC_SRC ${PROTO_GEN_DIR}/${PROTO_NAME}.grpc.pb.cc)
    set(GRPC_HDR ${PROTO_GEN_DIR}/${PROTO_NAME}.grpc.pb.h)

    add_custom_command(
        OUTPUT ${PROTO_SRC} ${PROTO_HDR} ${GRPC_SRC} ${GRPC_HDR}
        COMMAND protobuf::protoc
        ARGS --cpp_out=${PROTO_GEN_DIR}
             --grpc_out=${PROTO_GEN_DIR}
             --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
             -I${CMAKE_CURRENT_SOURCE_DIR}/proto
             ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating protobuf/gRPC for ${PROTO_NAME}"
    )

    list(APPEND PROTO_SRCS ${PROTO_SRC} ${GRPC_SRC})
    list(APPEND PROTO_HDRS ${PROTO_HDR} ${GRPC_HDR})
endforeach()

# Generated proto library
add_library(ifex_offboard_proto STATIC ${PROTO_SRCS})
target_include_directories(ifex_offboard_proto PUBLIC ${PROTO_GEN_DIR})
target_link_libraries(ifex_offboard_proto PUBLIC
    protobuf::libprotobuf
)
if(gRPC_FOUND)
    target_link_libraries(ifex_offboard_proto PUBLIC gRPC::grpc++)
else()
    find_library(GRPC_LIB grpc++)
    find_library(GRPC_REFLECTION grpc++_reflection)
    target_link_libraries(ifex_offboard_proto PUBLIC ${GRPC_LIB} ${GRPC_REFLECTION})
endif()

# Kafka client library
add_library(kafka_client STATIC
    libs/kafka_client/src/kafka_producer.cpp
    libs/kafka_client/src/kafka_consumer.cpp
)
target_include_directories(kafka_client PUBLIC
    libs/kafka_client/include
    ${RDKAFKA_INCLUDE_DIRS}
)
target_link_libraries(kafka_client PUBLIC
    ${RDKAFKA_LIBRARIES}
    glog::glog
)

# PostgreSQL client library
add_library(postgres_client STATIC
    libs/postgres_client/src/postgres_client.cpp
)
target_include_directories(postgres_client PUBLIC
    libs/postgres_client/include
    ${LIBPQ_INCLUDE_DIRS}
)
target_link_libraries(postgres_client PUBLIC
    ${LIBPQ_LIBRARIES}
    glog::glog
)

# Envelope codec library
add_library(envelope_codec STATIC
    libs/envelope_codec/src/discovery_codec.cpp
    libs/envelope_codec/src/scheduler_codec.cpp
    libs/envelope_codec/src/rpc_codec.cpp
    libs/envelope_codec/src/discovery_offboard_codec.cpp
    libs/envelope_codec/src/scheduler_offboard_codec.cpp
    libs/envelope_codec/src/rpc_offboard_codec.cpp
)
target_include_directories(envelope_codec PUBLIC
    libs/envelope_codec/include
)
target_link_libraries(envelope_codec PUBLIC
    ifex_offboard_proto
    glog::glog
)

# MQTT-Kafka Router library
add_library(mqtt_kafka_router STATIC
    libs/mqtt_kafka_router/src/mqtt_kafka_router.cpp
    libs/mqtt_kafka_router/src/vehicle_context.cpp
)
target_include_directories(mqtt_kafka_router PUBLIC
    libs/mqtt_kafka_router/include
    ${RDKAFKA_INCLUDE_DIRS}
)
target_link_libraries(mqtt_kafka_router PUBLIC
    ${RDKAFKA_LIBRARIES}
    ${PAHO_MQTT_CPP}
    ${PAHO_MQTT_C}
    glog::glog
)

# MQTT-Kafka Bridge service
add_executable(mqtt_kafka_bridge
    services/mqtt_kafka_bridge/main.cpp
)
target_link_libraries(mqtt_kafka_bridge PRIVATE
    mqtt_kafka_router
    envelope_codec
    glog::glog
    gflags
)

# Discovery Mirror service
add_executable(discovery_mirror
    services/discovery_mirror/main.cpp
    services/discovery_mirror/discovery_store.cpp
)
target_include_directories(discovery_mirror PRIVATE
    services/discovery_mirror
)
target_link_libraries(discovery_mirror PRIVATE
    kafka_client
    postgres_client
    envelope_codec
    glog::glog
    gflags
)

# Scheduler Mirror service
add_executable(scheduler_mirror
    services/scheduler_mirror/main.cpp
    services/scheduler_mirror/scheduler_store.cpp
)
target_include_directories(scheduler_mirror PRIVATE
    services/scheduler_mirror
)
target_link_libraries(scheduler_mirror PRIVATE
    kafka_client
    postgres_client
    envelope_codec
    glog::glog
    gflags
)

# RPC Gateway service
add_executable(rpc_gateway
    services/rpc_gateway/main.cpp
    services/rpc_gateway/rpc_store.cpp
)
target_include_directories(rpc_gateway PRIVATE
    services/rpc_gateway
)
target_link_libraries(rpc_gateway PRIVATE
    kafka_client
    postgres_client
    envelope_codec
    glog::glog
    gflags
)

# Tests
if(BUILD_TESTS)
    enable_testing()
    find_package(GTest REQUIRED)
    add_subdirectory(tests)
endif()

# Install targets
install(TARGETS
    mqtt_kafka_bridge
    discovery_mirror
    scheduler_mirror
    rpc_gateway
    RUNTIME DESTINATION bin
)
