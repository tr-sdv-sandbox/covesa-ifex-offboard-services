# Offboard Platform Library
#
# Provides the OffboardTransport abstraction for cloud services.
# Implements the IFEX cloud_backend_transport_service API using Kafka + MQTT.

find_package(Threads REQUIRED)

# Get gRPC plugin (from parent CMakeLists.txt or find it)
if(NOT GRPC_CPP_PLUGIN)
    find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

# Proto generation - IFEX-generated service APIs
# All protos are generated from IFEX YAML via ./generate_proto.sh
set(OFFBOARD_PLATFORM_PROTOS
    ${CMAKE_CURRENT_SOURCE_DIR}/cloud-backend-transport-service.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/cloud-dispatcher-service.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/cloud-scheduler-service.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/cloud-discovery-service.proto
)

# Generate C++ from protos (protobuf + gRPC)
set(OFFBOARD_PLATFORM_PROTO_SRCS
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-backend-transport-service.pb.cc
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-dispatcher-service.pb.cc
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-scheduler-service.pb.cc
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-discovery-service.pb.cc
)
set(OFFBOARD_PLATFORM_PROTO_HDRS
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-backend-transport-service.pb.h
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-dispatcher-service.pb.h
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-scheduler-service.pb.h
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-discovery-service.pb.h
)
set(OFFBOARD_PLATFORM_GRPC_SRCS
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-backend-transport-service.grpc.pb.cc
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-dispatcher-service.grpc.pb.cc
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-scheduler-service.grpc.pb.cc
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-discovery-service.grpc.pb.cc
)
set(OFFBOARD_PLATFORM_GRPC_HDRS
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-backend-transport-service.grpc.pb.h
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-dispatcher-service.grpc.pb.h
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-scheduler-service.grpc.pb.h
    ${CMAKE_CURRENT_BINARY_DIR}/cloud-discovery-service.grpc.pb.h
)

# Generate each proto file
foreach(PROTO_FILE ${OFFBOARD_PLATFORM_PROTOS})
    get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
    add_custom_command(
        OUTPUT
            ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.cc
            ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.pb.h
            ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.cc
            ${CMAKE_CURRENT_BINARY_DIR}/${PROTO_NAME}.grpc.pb.h
        COMMAND protobuf::protoc
        ARGS --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
             --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
             --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
             -I${CMAKE_CURRENT_SOURCE_DIR}
             ${PROTO_FILE}
        DEPENDS ${PROTO_FILE}
        COMMENT "Generating C++ and gRPC from ${PROTO_NAME}.proto"
    )
endforeach()

# Library sources
set(OFFBOARD_PLATFORM_SOURCES
    src/kafka_mqtt_transport.cpp
    src/cloud_backend_transport_server.cpp
    ${OFFBOARD_PLATFORM_PROTO_SRCS}
    ${OFFBOARD_PLATFORM_GRPC_SRCS}
)

# Create the library
add_library(offboard_platform STATIC ${OFFBOARD_PLATFORM_SOURCES})

target_include_directories(offboard_platform
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_CURRENT_BINARY_DIR}  # For generated proto/gRPC headers
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# Link libraries - gRPC from parent scope
if(gRPC_FOUND)
    target_link_libraries(offboard_platform
        PUBLIC
            kafka_client
            protobuf::libprotobuf
            gRPC::grpc++
            glog::glog
        PRIVATE
            Threads::Threads
    )
else()
    find_library(GRPC_LIB grpc++)
    target_link_libraries(offboard_platform
        PUBLIC
            kafka_client
            protobuf::libprotobuf
            ${GRPC_LIB}
            glog::glog
        PRIVATE
            Threads::Threads
    )
endif()

# Alias for consistency
add_library(ifex::offboard_platform ALIAS offboard_platform)

# Backend Transport Server executable
add_executable(backend_transport_server
    src/backend_transport_server_main.cpp
)

target_link_libraries(backend_transport_server
    PRIVATE
        offboard_platform
        gflags
        glog::glog
)

# Install headers
install(
    DIRECTORY include/
    DESTINATION include/offboard-platform
    FILES_MATCHING PATTERN "*.hpp"
)

# Install executable
install(TARGETS backend_transport_server RUNTIME DESTINATION bin)
