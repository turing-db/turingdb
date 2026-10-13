add_executable(test_query_v3_test_suite
    V3QueryTestSuite.cpp
    V3QueryTestRunner.cpp
    QueryTestRunner.cpp
    QueryResultFormatter.cpp)

target_link_libraries(test_query_v3_test_suite
    PRIVATE turing_test_libs)

gtest_discover_tests(test_query_v3_test_suite
      DISCOVERY_TIMEOUT 10
      DISCOVERY_MODE PRE_TEST)

set(QUERY_TEST_SUITE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests)
set(QUERY_TEST_SUITE_DATA_DIR ${CMAKE_CURRENT_SOURCE_DIR}/data)

target_compile_definitions(test_query_v3_test_suite
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}"
            QUERY_TEST_SUITE_DATA_DIR="${QUERY_TEST_SUITE_DATA_DIR}")

add_executable(query_test_suite_cli
    QueryTestSuiteCLI.cpp
    QueryTestRunner.cpp
    V3QueryTestRunner.cpp
    QueryResultFormatter.cpp)

target_link_libraries(query_test_suite_cli
    PRIVATE turing_test_libs)

target_compile_definitions(query_test_suite_cli
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}"
            QUERY_TEST_SUITE_DATA_DIR="${QUERY_TEST_SUITE_DATA_DIR}")
