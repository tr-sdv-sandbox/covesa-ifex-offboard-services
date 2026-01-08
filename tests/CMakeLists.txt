# Tests for covesa-ifex-offboard-services

include(GoogleTest)

# =============================================================================
# E2E Test Infrastructure Library
# =============================================================================
# Shared fixture for E2E tests with real vehicle containers
add_library(e2e_test_fixture STATIC
    e2e/e2e_test_fixture.cpp
)
target_include_directories(e2e_test_fixture PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/e2e
    ${CMAKE_SOURCE_DIR}/libs/postgres_client/include
)
target_compile_definitions(e2e_test_fixture PRIVATE
    SCHEMA_SQL_PATH="${CMAKE_SOURCE_DIR}/sql/schema.sql"
)
target_link_libraries(e2e_test_fixture PUBLIC
    postgres_client
    GTest::gtest
    glog::glog
)

# Unit tests for scheduler command serialization
add_executable(scheduler_command_test
    unit/scheduler_command_test.cpp
)
target_include_directories(scheduler_command_test PRIVATE
    ${CMAKE_SOURCE_DIR}/services/cloud_scheduler
    ${CMAKE_SOURCE_DIR}/libs/envelope_codec/include
)
target_link_libraries(scheduler_command_test PRIVATE
    ifex_offboard_proto
    envelope_codec
    GTest::gtest
    GTest::gtest_main
    glog::glog
)
gtest_discover_tests(scheduler_command_test)

# Integration tests (require Kafka/PostgreSQL)
# add_subdirectory(integration)

# End-to-end tests (require full infrastructure)
# These tests connect to running Cloud Scheduler and verify full command flow
# Run ./deploy/test-scheduler-e2e.sh --keep first to start infrastructure
add_executable(scheduler_e2e_test
    e2e/scheduler_e2e_test.cpp
)
target_include_directories(scheduler_e2e_test PRIVATE
    ${CMAKE_SOURCE_DIR}/services/cloud_scheduler
    ${CMAKE_SOURCE_DIR}/libs/envelope_codec/include
    ${CMAKE_SOURCE_DIR}/libs/postgres_client/include
)
target_link_libraries(scheduler_e2e_test PRIVATE
    ifex_offboard_proto
    envelope_codec
    postgres_client
    e2e_test_fixture
    GTest::gtest
    GTest::gtest_main
    glog::glog
    grpc++
)

# Don't use gtest_discover_tests - it runs each test as separate process
# which causes SetUpTestSuite to run for EVERY test (slow startup per test)
# Instead, run as single binary to share the fixture
add_test(NAME scheduler_e2e COMMAND scheduler_e2e_test)
set_tests_properties(scheduler_e2e PROPERTIES LABELS "e2e;docker")

# Status (is_online) E2E tests
# Tests vehicle online/offline status tracking via MQTT → PostgreSQL
# Run ./deploy/start-infra.sh first, then: ctest -R status_e2e
add_executable(status_e2e_test
    e2e/status_e2e_test.cpp
)
target_include_directories(status_e2e_test PRIVATE
    ${CMAKE_SOURCE_DIR}/libs/postgres_client/include
)

# Find mosquitto library
find_library(MOSQUITTO_LIB mosquitto REQUIRED)

target_link_libraries(status_e2e_test PRIVATE
    postgres_client
    e2e_test_fixture
    GTest::gtest
    GTest::gtest_main
    glog::glog
    ${MOSQUITTO_LIB}
)

# Don't use gtest_discover_tests - it runs each test as separate process
# which causes SetUpTestSuite to run for EVERY test (slow startup per test)
# Instead, run as single binary to share the fixture
add_test(NAME status_e2e COMMAND status_e2e_test)
set_tests_properties(status_e2e PROPERTIES LABELS "e2e;docker")

# Discovery Service E2E tests
# Tests Cloud Discovery Service gRPC API against PostgreSQL
# Run ./deploy/start-infra.sh first, then: ctest -R discovery_e2e
add_executable(discovery_e2e_test
    e2e/discovery_e2e_test.cpp
)
target_include_directories(discovery_e2e_test PRIVATE
    ${CMAKE_SOURCE_DIR}/services/api/discovery
    ${CMAKE_SOURCE_DIR}/libs/postgres_client/include
)
target_link_libraries(discovery_e2e_test PRIVATE
    ifex_offboard_proto
    postgres_client
    e2e_test_fixture
    GTest::gtest
    GTest::gtest_main
    glog::glog
    grpc++
)

# Don't use gtest_discover_tests - it runs each test as separate process
# which causes SetUpTestSuite to run for EVERY test (slow startup per test)
# Instead, run as single binary to share the fixture
add_test(NAME discovery_e2e COMMAND discovery_e2e_test)
set_tests_properties(discovery_e2e PROPERTIES LABELS "e2e;docker")

# Dispatcher (RPC) Service E2E tests
# Tests Cloud Dispatcher Service gRPC API for in-memory RPC tracking
# Run ./deploy/start-infra.sh first, then: ctest -R dispatcher_e2e
add_executable(dispatcher_e2e_test
    e2e/dispatcher_e2e_test.cpp
)
target_include_directories(dispatcher_e2e_test PRIVATE
    ${CMAKE_SOURCE_DIR}/services/api/dispatcher
    ${CMAKE_SOURCE_DIR}/libs/envelope_codec/include
    ${CMAKE_SOURCE_DIR}/libs/postgres_client/include
    ${RDKAFKA_INCLUDE_DIRS}
)
target_link_libraries(dispatcher_e2e_test PRIVATE
    ifex_offboard_proto
    envelope_codec
    postgres_client
    e2e_test_fixture
    ${RDKAFKA_LIBRARIES}
    GTest::gtest
    GTest::gtest_main
    glog::glog
    grpc++
)

# Don't use gtest_discover_tests - it runs each test as separate process
# which causes SetUpTestSuite to run for EVERY test (slow startup per test)
# Instead, run as single binary to share the fixture
add_test(NAME dispatcher_e2e COMMAND dispatcher_e2e_test)
set_tests_properties(dispatcher_e2e PROPERTIES LABELS "e2e;docker")
