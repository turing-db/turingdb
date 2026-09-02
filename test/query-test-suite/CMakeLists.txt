function(add_query_suite_target exe_name)
    add_executable(${exe_name} ${ARGN})
    target_link_libraries(${exe_name}
        PRIVATE turing_common_s
                spdlog::spdlog
                argparse
                turing_db_parser_s
                turing_db_analyzer_s
                turing_db_ast_s
                turing_db_system_s
                turing_db_memory_s
                turing_db_plan_s
                turing_db_optimizer_s
                turing_db_examples_s
                turing_db_s
                turing_testenv_s
                turing_testutils_s)
endfunction()

set(V3_QUERY_SUITE_LIBS
    turing_db_interpreter_v3_s
    turing_db_ir_codegen_s
    turing_db_ir_dbdialect_s
    turing_db_ir_nldialect_s
    turing_db_ir_storagedialect_s
    turing_db_ir_interpreter_s
    turing_db_ir_common_s
    turing_db_frontend_cypher_s)

add_query_suite_target(test_query_test_suite
    QueryTestSuite.cpp
    QueryTestRunner.cpp
    QueryResultFormatter.cpp)

gtest_discover_tests(test_query_test_suite
      DISCOVERY_TIMEOUT 10
      DISCOVER_MODE PRE_TEST)

add_query_suite_target(test_remote_query_test_suite
    RemoteQueryTestSuite.cpp
    QueryTestRunner.cpp
    RemoteQueryTestRunner.cpp
    QueryResultFormatter.cpp)

target_link_libraries(test_remote_query_test_suite
    PRIVATE turing_db_server_s
            turing_db_proto_client_s)

gtest_discover_tests(test_remote_query_test_suite
      DISCOVERY_TIMEOUT 10
      DISCOVER_MODE PRE_TEST)

add_query_suite_target(test_query_v3_test_suite
    V3QueryTestSuite.cpp
    V3QueryTestRunner.cpp
    QueryTestRunner.cpp
    QueryResultFormatter.cpp)

target_link_libraries(test_query_v3_test_suite
    PRIVATE ${V3_QUERY_SUITE_LIBS})

gtest_discover_tests(test_query_v3_test_suite
      DISCOVERY_TIMEOUT 10
      DISCOVER_MODE PRE_TEST)

add_query_suite_target(query_test_suite_cli
    QueryTestSuiteCLI.cpp
    QueryTestRunner.cpp
    RemoteQueryTestRunner.cpp
    V3QueryTestRunner.cpp
    QueryResultFormatter.cpp)

target_link_libraries(query_test_suite_cli
    PRIVATE turing_db_server_s
            turing_db_proto_client_s
            ${V3_QUERY_SUITE_LIBS})

set(QUERY_TEST_SUITE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tests)

target_compile_definitions(test_query_test_suite
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}")

target_compile_definitions(test_remote_query_test_suite
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}")

target_compile_definitions(test_query_v3_test_suite
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}")

target_compile_definitions(query_test_suite_cli
    PRIVATE QUERY_TEST_SUITE_DIR="${QUERY_TEST_SUITE_DIR}")
