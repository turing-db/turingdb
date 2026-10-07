set(IR_TEST_DIR ${CMAKE_CURRENT_SOURCE_DIR})

function(add_ir_tests exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE
            turing_common_s
            turing_db_storage_s
            turing_db_jobs_s
            turing_db_ir_exec_s
            turing_db_ir_lowering_s
            turing_db_ir_dbdialect_s
            turing_db_ir_common_s
            MLIRParser)
    target_include_directories(${exe_name} PRIVATE ${IR_TEST_DIR})
endfunction()

# The output-row rendering and the count / row sinks that several of the tests below
# would otherwise each redeclare.
add_library(turing_ir_test_rows_s STATIC IRTestRows.cpp)
target_link_libraries(turing_ir_test_rows_s
    PUBLIC  GTest::gtest
            turing_db_s
            turing_db_storage_s
            turing_db_ir_exec_s)
target_include_directories(turing_ir_test_rows_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# The change / write / read plumbing every test of a writing query needs, over the shared
# SimpleGraph fixture.
add_library(turing_ir_test_write_query_s STATIC WriteQueryTest.cpp)
target_link_libraries(turing_ir_test_write_query_s
    PUBLIC  turing_ir_test_rows_s
            turing_testutils_s
            turing_testenv_s
            turing_db_s
            turing_db_examples_s
            turing_db_system_s
            turing_db_storage_s
            turing_db_ir_exec_s)
target_include_directories(turing_ir_test_write_query_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Compiles a query against the shared SimpleGraph with the join forced, for the hash join
# tests that read the db program, the nl program or the rows it runs to.
add_library(turing_ir_test_hash_join_s STATIC HashJoinQueryTest.cpp ProductPlacement.cpp StringRowSink.cpp)
target_link_libraries(turing_ir_test_hash_join_s
    PUBLIC  GTest::gtest
            turing_testutils_s
            turing_db_storage_s
            turing_db_examples_s
            turing_db_ir_codegen_s
            turing_db_ir_dbdialect_s
            turing_db_ir_lowering_s
            turing_db_ir_exec_s
            turing_db_ir_common_s
            turing_db_frontend_cypher_s
            turing_db_parser_s
            turing_db_analyzer_s
            turing_db_ast_s
            turing_db_procedures_s)
target_include_directories(turing_ir_test_hash_join_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# The CALL gap tests drive the whole engine through QueryInterpreterV3 on the simpledb
# fixture, writes included, and read their rows as text through StringRowSink.
function(add_call_v3_test exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_sources(${exe_name} PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
    target_link_libraries(${exe_name} PRIVATE
            turing_db_s
            turing_testenv_s
            turing_db_examples_s
            turing_db_system_s
            turing_db_storage_s
            turing_db_ir_exec_s)
    target_include_directories(${exe_name} PRIVATE ${IR_TEST_DIR})
endfunction()

add_subdirectory(aggregate)
add_subdirectory(call)
add_subdirectory(case)
add_subdirectory(conditional)
add_subdirectory(engine)
add_subdirectory(expression)
add_subdirectory(fuzz)
add_subdirectory(join)
add_subdirectory(ldbc)
add_subdirectory(list)
add_subdirectory(load)
add_subdirectory(map)
add_subdirectory(match)
add_subdirectory(merge)
add_subdirectory(null)
add_subdirectory(optional_match)
add_subdirectory(passes)
add_subdirectory(path)
add_subdirectory(projection)
add_subdirectory(set)
add_subdirectory(subquery)
add_subdirectory(system)
add_subdirectory(temporal)
add_subdirectory(union)
add_subdirectory(unwind)
add_subdirectory(vector)
add_subdirectory(with)
add_subdirectory(write)
