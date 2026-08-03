function(add_ir_tests exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE
            turing_common_s
            turing_db_storage_s
            turing_db_jobs_s
            turing_db_ir_interpreter_s
            turing_db_ir_lowering_s
            turing_db_ir_dbdialect_s
            turing_db_ir_common_s
            MLIRParser)
endfunction()

add_ir_tests(test_query_ir_nlexecutor NLExecutorTest.cpp)
add_ir_tests(test_query_ir_create_node_edge CreateNodeEdgeTest.cpp)
add_ir_tests(test_query_ir_dblowering DBLoweringTest.cpp)
# The nested cross product execution tests run against the shared SimpleGraph fixture.
target_link_libraries(test_query_ir_dblowering PRIVATE turing_db_examples_s)
add_ir_tests(test_query_ir_group_aggregate GroupAggregateTest.cpp)
target_link_libraries(test_query_ir_group_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_interpreter_v3_s)
add_ir_tests(test_query_ir_collect CollectTest.cpp)

# The collect row-alignment and drain tests run against the shared SimpleGraph fixture.
add_ir_tests(test_query_ir_collect_row_alignment CollectRowAlignmentTest.cpp)
target_link_libraries(test_query_ir_collect_row_alignment PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_collect_drain CollectDrainTest.cpp)
target_link_libraries(test_query_ir_collect_drain PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_collect_result_type CollectResultTypeTest.cpp)
add_ir_tests(test_query_ir_dbdialect DBDialectTest.cpp)
add_ir_tests(test_query_ir_nldialect NLDialectTest.cpp)

# The branch two-hop test runs against the shared SimpleGraph fixture.
add_ir_tests(test_query_ir_branch_two_hop BranchTwoHopTest.cpp)
target_link_libraries(test_query_ir_branch_two_hop PRIVATE turing_db_examples_s)

add_turing_test(test_query_ir_where_expr_registration WhereExprRegistrationTest.cpp)
target_link_libraries(test_query_ir_where_expr_registration PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_where_expr_registration PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_create_equivalence CreateEquivalenceTest.cpp)
target_link_libraries(test_query_ir_create_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_create_equivalence PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_interpreter_v3_error QueryInterpreterV3ErrorTest.cpp)
target_link_libraries(test_query_ir_interpreter_v3_error PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_interpreter_v3_error PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_set_equivalence SetEquivalenceTest.cpp)
target_link_libraries(test_query_ir_set_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_set_equivalence PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_delete_equivalence DeleteEquivalenceTest.cpp)
target_link_libraries(test_query_ir_delete_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_delete_equivalence PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_equivalence EquivalenceTest.cpp)
target_link_libraries(test_query_ir_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_interpreter_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_equivalence PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_order_by OrderByTest.cpp)
target_link_libraries(test_query_ir_order_by PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_interpreter_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_order_by PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_chained_call ChainedCallTest.cpp)
target_link_libraries(test_query_ir_chained_call PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_interpreter_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_chained_call PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_cypher_output CypherOutputTest.cpp)
target_link_libraries(test_query_ir_cypher_output PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_interpreter_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_cypher_output PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_wildcard_return WildcardReturnTest.cpp)
target_link_libraries(test_query_ir_wildcard_return PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_interpreter_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_wildcard_return PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
