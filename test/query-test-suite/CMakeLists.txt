set(V3_QUERY_SUITE_LIBS
    turing_common_s
    turing_db_s
    turing_db_interpreter_s
    turing_db_ir_exec_s
    turing_db_system_s
    turing_db_storage_s
    turing_db_memory_s
    turing_db_net_encoders
    turing_db_examples_s
    turing_testenv_s
    turing_testutils_s)

add_executable(test_query_v3_test_suite
    V3QueryTestSuite.cpp
    V3QueryTestRunner.cpp
    QueryTestRunner.cpp
    QueryResultFormatter.cpp)

target_link_libraries(test_query_v3_test_suite
    PRIVATE ${V3_QUERY_SUITE_LIBS})

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
    PRIVATE ${V3_QUERY_SUITE_LIBS})

target_compile_definitions(query_test_suite_cli
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}"
            QUERY_TEST_SUITE_DATA_DIR="${QUERY_TEST_SUITE_DATA_DIR}")
