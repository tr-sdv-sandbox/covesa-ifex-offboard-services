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
