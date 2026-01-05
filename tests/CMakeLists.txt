# Tests for covesa-ifex-offboard-services

include(GoogleTest)

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
)
target_link_libraries(scheduler_e2e_test PRIVATE
    ifex_offboard_proto
    envelope_codec
    GTest::gtest
    GTest::gtest_main
    glog::glog
    grpc++
)

# Don't auto-discover e2e tests - they require infrastructure
# Run manually: ctest -R scheduler_e2e
gtest_discover_tests(scheduler_e2e_test
    PROPERTIES LABELS "e2e;docker"
)

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
    GTest::gtest
    GTest::gtest_main
    glog::glog
    ${MOSQUITTO_LIB}
)

gtest_discover_tests(status_e2e_test
    PROPERTIES LABELS "e2e;docker"
)
