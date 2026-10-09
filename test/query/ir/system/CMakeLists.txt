add_turing_test(test_query_ir_system_command_equivalence SystemCommandEquivalenceTest.cpp)
target_link_libraries(test_query_ir_system_command_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_system_command_equivalence PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_system_command_context SystemCommandContextTest.cpp)

add_ir_tests(test_query_ir_system_ops_dialect SystemOpsDialectTest.cpp)
target_compile_definitions(test_query_ir_system_ops_dialect
    PRIVATE MLIR_SAMPLES_DIR="${CMAKE_SOURCE_DIR}/samples/mlir")

add_ir_tests(test_query_ir_system_command_lowering SystemCommandLoweringTest.cpp)
target_compile_definitions(test_query_ir_system_command_lowering
    PRIVATE MLIR_SAMPLES_DIR="${CMAKE_SOURCE_DIR}/samples/mlir")

add_turing_test(test_query_ir_system_command SystemCommandTest.cpp)
target_link_libraries(test_query_ir_system_command PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_system_command PRIVATE ${IR_TEST_DIR})

# A graph load that fails after opening the change it imports into.
add_turing_test(test_query_ir_failed_graph_load FailedGraphLoadTest.cpp)
target_sources(test_query_ir_failed_graph_load PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_failed_graph_load PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_failed_graph_load PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_graph_not_found GraphNotFoundTest.cpp)
target_sources(test_query_ir_graph_not_found PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_graph_not_found PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_graph_not_found PRIVATE ${IR_TEST_DIR})
