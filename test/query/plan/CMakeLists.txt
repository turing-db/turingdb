function(add_v2_plan_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_v2_parser_s
                turing_db_v2_analyzer_s
                turing_db_v2_ast_s
                turing_db_system_s
                turing_db_memory_s
                turing_db_v2_plan_s
                turing_db_s
                turing_testenv_s
                tabulate)
endfunction()

add_v2_plan_gtest(test_v2_plangen PlanGenTest.cpp)

add_v2_plan_gtest(test_v2_plangenerator PlanGeneratorTest.cpp)

set(SUCCESS_READ_QUERIES_DIR ${CMAKE_CURRENT_BINARY_DIR}/success-reads)
set(SUCCESS_WRITE_QUERIES_DIR ${CMAKE_CURRENT_BINARY_DIR}/success-writes)
set(FAIL_READ_QUERIES_DIR ${CMAKE_CURRENT_BINARY_DIR}/fail-reads)
set(FAIL_WRITE_QUERIES_DIR ${CMAKE_CURRENT_BINARY_DIR}/fail-writes)

target_compile_definitions(test_v2_plangenerator
    PRIVATE SUCCESS_READ_QUERIES_DIR="${SUCCESS_READ_QUERIES_DIR}"
            SUCCESS_WRITE_QUERIES_DIR="${SUCCESS_WRITE_QUERIES_DIR}"
            FAIL_READ_QUERIES_DIR="${FAIL_READ_QUERIES_DIR}"
            FAIL_WRITE_QUERIES_DIR="${FAIL_WRITE_QUERIES_DIR}")

execute_process(
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_CURRENT_SOURCE_DIR}/success-reads
            ${CMAKE_CURRENT_BINARY_DIR}/success-reads)

execute_process(
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_CURRENT_SOURCE_DIR}/success-writes
            ${CMAKE_CURRENT_BINARY_DIR}/success-writes)

execute_process(
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_CURRENT_SOURCE_DIR}/fail-reads
            ${CMAKE_CURRENT_BINARY_DIR}/fail-reads)

execute_process(
    COMMAND ${CMAKE_COMMAND} -E copy_directory
            ${CMAKE_CURRENT_SOURCE_DIR}/fail-writes
            ${CMAKE_CURRENT_BINARY_DIR}/fail-writes)
