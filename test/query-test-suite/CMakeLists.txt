# Renders a result as the text the fixtures expect, for this suite and the tests
# that read results over the binary protocol.
add_library(turing_query_result_formatter_s STATIC QueryResultFormatter.cpp)
target_link_libraries(turing_query_result_formatter_s
    PUBLIC  turing_db_storage_s
            turing_db_query_common_s)
target_include_directories(turing_query_result_formatter_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(test_query_v3_test_suite
    V3QueryTestSuite.cpp
    V3QueryTestRunner.cpp
    QueryTestRunner.cpp)

target_link_libraries(test_query_v3_test_suite
    PRIVATE turing_test_libs)

gtest_discover_tests(test_query_v3_test_suite
      DISCOVERY_TIMEOUT 10
      DISCOVERY_MODE PRE_TEST)

set(QUERY_TEST_SUITE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests)
set(QUERY_TEST_SUITE_DATA_DIR ${CMAKE_CURRENT_SOURCE_DIR}/data)
set(QUERY_TEST_SUITE_GRAPHS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/graphs)

target_compile_definitions(test_query_v3_test_suite
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}"
            QUERY_TEST_SUITE_DATA_DIR="${QUERY_TEST_SUITE_DATA_DIR}"
            QUERY_TEST_SUITE_GRAPHS_DIR="${QUERY_TEST_SUITE_GRAPHS_DIR}")

add_turing_test(test_query_test_suite_split_statements SplitStatementsTest.cpp)
target_sources(test_query_test_suite_split_statements PRIVATE QueryTestRunner.cpp)

add_executable(query_test_suite_cli
    QueryTestSuiteCLI.cpp
    QueryTestRunner.cpp
    V3QueryTestRunner.cpp)

target_link_libraries(query_test_suite_cli
    PRIVATE turing_test_libs)

target_compile_definitions(query_test_suite_cli
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}"
            QUERY_TEST_SUITE_DATA_DIR="${QUERY_TEST_SUITE_DATA_DIR}"
            QUERY_TEST_SUITE_GRAPHS_DIR="${QUERY_TEST_SUITE_GRAPHS_DIR}")
