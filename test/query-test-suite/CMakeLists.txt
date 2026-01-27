function(add_query_suite_target exe_name)
    add_executable(${exe_name} ${ARGN})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_parser_s
                turing_db_analyzer_s
                turing_db_ast_s
                turing_db_system_s
                turing_db_memory_s
                turing_db_plan_s
                turing_db_examples_s
                turing_db_s
                turing_testenv_s
                turing_testutils_s)
endfunction()

add_query_suite_target(test_query_test_suite
    QueryTestSuite.cpp
    QueryTestRunner.cpp)

gtest_discover_tests(test_query_test_suite
      DISCOVERY_TIMEOUT 10
      DISCOVER_MODE PRE_TEST)

add_query_suite_target(query_test_suite_cli
    QueryTestSuiteCLI.cpp
    QueryTestRunner.cpp)

set(QUERY_TEST_SUITE_DIR ${CMAKE_CURRENT_BINARY_DIR}/tests)

target_compile_definitions(test_query_test_suite
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}")

target_compile_definitions(query_test_suite_cli
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}")

execute_process(
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_CURRENT_SOURCE_DIR}/tests
            ${CMAKE_CURRENT_BINARY_DIR}/tests)
