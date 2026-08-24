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

add_turing_test(test_query_ir_function_input_column FunctionInputColumnTest.cpp)
target_link_libraries(test_query_ir_function_input_column PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_function_input_column PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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

add_turing_test(test_query_ir_cross_product_cut CrossProductCutTest.cpp)
target_link_libraries(test_query_ir_cross_product_cut PRIVATE
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
target_include_directories(test_query_ir_cross_product_cut PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_order_by_alias_key OrderByAliasKeyTest.cpp)
target_link_libraries(test_query_ir_order_by_alias_key PRIVATE
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
target_include_directories(test_query_ir_order_by_alias_key PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_order_by_constant_alias_key OrderByConstantAliasKeyTest.cpp)
target_link_libraries(test_query_ir_order_by_constant_alias_key PRIVATE
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
target_include_directories(test_query_ir_order_by_constant_alias_key PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_constant_alias_expr_column ConstantAliasExprColumnTest.cpp)
target_link_libraries(test_query_ir_constant_alias_expr_column PRIVATE
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
target_include_directories(test_query_ir_constant_alias_expr_column PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_order_by_count OrderByCountTest.cpp)
target_link_libraries(test_query_ir_order_by_count PRIVATE
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
target_include_directories(test_query_ir_order_by_count PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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

add_turing_test(test_query_ir_constant_output ConstantOutputTest.cpp)
target_link_libraries(test_query_ir_constant_output PRIVATE
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
target_include_directories(test_query_ir_constant_output PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_arithmetic_ext ArithmeticExtTest.cpp)
target_link_libraries(test_query_ir_arithmetic_ext PRIVATE
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
target_include_directories(test_query_ir_arithmetic_ext PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_group_aggregate_arithmetic GroupAggregateArithmeticTest.cpp)
target_link_libraries(test_query_ir_group_aggregate_arithmetic PRIVATE
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
target_include_directories(test_query_ir_group_aggregate_arithmetic PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_group_aggregate_expression GroupAggregateExpressionTest.cpp)
target_link_libraries(test_query_ir_group_aggregate_expression PRIVATE
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
target_include_directories(test_query_ir_group_aggregate_expression PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_distinct DistinctTest.cpp)
target_link_libraries(test_query_ir_distinct PRIVATE
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
target_include_directories(test_query_ir_distinct PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_count_distinct CountDistinctTest.cpp)
target_link_libraries(test_query_ir_count_distinct PRIVATE
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
target_include_directories(test_query_ir_count_distinct PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_cypher_unwind CypherUnwindTest.cpp)
target_link_libraries(test_query_ir_cypher_unwind PRIVATE
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
target_include_directories(test_query_ir_cypher_unwind PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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

add_turing_test(test_query_ir_nested_aggregate_projection NestedAggregateProjectionTest.cpp)
target_link_libraries(test_query_ir_nested_aggregate_projection PRIVATE
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
target_include_directories(test_query_ir_nested_aggregate_projection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_output_column_names OutputColumnNamesTest.cpp)
target_link_libraries(test_query_ir_output_column_names PRIVATE
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
target_include_directories(test_query_ir_output_column_names PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_limit_skip LimitSkipTest.cpp)
target_link_libraries(test_query_ir_limit_skip PRIVATE
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
target_include_directories(test_query_ir_limit_skip PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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

add_turing_test(test_query_ir_unwind_constant_projection UnwindConstantProjectionTest.cpp)
target_link_libraries(test_query_ir_unwind_constant_projection PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_unwind_constant_projection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_constant_only_projection ConstantOnlyProjectionTest.cpp)
target_link_libraries(test_query_ir_constant_only_projection PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_constant_only_projection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_aggregate_over_constant_alias AggregateOverConstantAliasTest.cpp)
target_link_libraries(test_query_ir_aggregate_over_constant_alias PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_aggregate_over_constant_alias PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with WithTest.cpp)
target_link_libraries(test_query_ir_with PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_with PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_ldbc_bi11 LdbcBi11Test.cpp)
target_link_libraries(test_query_ir_ldbc_bi11 PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_ldbc_bi11 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with_multi_part WithMultiPartTest.cpp)
target_link_libraries(test_query_ir_with_multi_part PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_with_multi_part PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_order_by_aggregate_unprojected_key OrderByAggregateUnprojectedKeyTest.cpp)
target_link_libraries(test_query_ir_order_by_aggregate_unprojected_key PRIVATE
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
target_include_directories(test_query_ir_order_by_aggregate_unprojected_key PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_constant_predicate_projection ConstantPredicateProjectionTest.cpp)
target_link_libraries(test_query_ir_constant_predicate_projection PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_constant_predicate_projection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_count_wildcard_input CountWildcardInputTest.cpp)
target_link_libraries(test_query_ir_count_wildcard_input PRIVATE
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
target_include_directories(test_query_ir_count_wildcard_input PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_count_distinct_ordering CountDistinctOrderingTest.cpp)
target_link_libraries(test_query_ir_count_distinct_ordering PRIVATE
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
target_include_directories(test_query_ir_count_distinct_ordering PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_count_distinct_argument CountDistinctArgumentTest.cpp)
target_link_libraries(test_query_ir_count_distinct_argument PRIVATE
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
target_include_directories(test_query_ir_count_distinct_argument PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_count_distinct_grouping CountDistinctGroupingTest.cpp)
target_link_libraries(test_query_ir_count_distinct_grouping PRIVATE
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
target_include_directories(test_query_ir_count_distinct_grouping PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_count_distinct_value CountDistinctValueTest.cpp)
target_link_libraries(test_query_ir_count_distinct_value PRIVATE
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
target_include_directories(test_query_ir_count_distinct_value PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
