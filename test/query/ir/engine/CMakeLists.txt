add_ir_tests(test_query_ir_nlexecutor NLExecutorTest.cpp)

add_ir_tests(test_query_ir_dblowering DBLoweringTest.cpp)
# The nested cross product execution tests run against the shared SimpleGraph fixture.
target_link_libraries(test_query_ir_dblowering PRIVATE turing_db_examples_s)

add_call_v3_test(test_query_ir_empty_result_column_names EmptyResultColumnNamesTest.cpp)

add_ir_tests(test_query_ir_dbdialect DBDialectTest.cpp)

add_ir_tests(test_query_ir_nldialect NLDialectTest.cpp)

# The reduced-row output tests read the single row a reduction collapsed to from another
# block, against the shared SimpleGraph fixture.
add_ir_tests(test_query_ir_reduced_row_output ReducedRowOutputTest.cpp)
target_link_libraries(test_query_ir_reduced_row_output PRIVATE turing_db_examples_s)

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
target_include_directories(test_query_ir_where_expr_registration PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_exec_v3_error QueryInterpreterV3ErrorTest.cpp)
target_link_libraries(test_query_ir_exec_v3_error PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_exec_v3_error PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_equivalence PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_cypher_output PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_output_column_names PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_variable_type_conflict VariableTypeConflictTest.cpp)
target_link_libraries(test_query_ir_variable_type_conflict PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_variable_type_conflict PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_unknown_variable UnknownVariableTest.cpp)
target_link_libraries(test_query_ir_unknown_variable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_unknown_variable PRIVATE ${IR_TEST_DIR})

# The EXPLAIN dumps run the whole compilation of a query against the shared SimpleGraph
# fixture, so the test needs the db dialect - whose passes the prefix reports on -
# alongside the engine.
add_ir_tests(test_query_ir_explain ExplainTest.cpp)
target_sources(test_query_ir_explain PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_explain PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_explain PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_complex_composition ComplexCompositionTest.cpp)
target_link_libraries(test_query_ir_complex_composition PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_complex_composition PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_mask_column_carry MaskColumnCarryTest.cpp)
target_link_libraries(test_query_ir_mask_column_carry PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_mask_column_carry PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_signed_number_lexing SignedNumberLexingTest.cpp)
target_link_libraries(test_query_ir_signed_number_lexing PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_signed_number_lexing PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_keyword_named_identifier KeywordNamedIdentifierTest.cpp)
target_sources(test_query_ir_keyword_named_identifier PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_keyword_named_identifier PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_keyword_named_identifier PRIVATE ${IR_TEST_DIR})

# Expressions nested up to and past the parser's maximum depth.
add_call_v3_test(test_query_ir_deep_expression DeepExpressionTest.cpp)
target_link_libraries(test_query_ir_deep_expression PRIVATE turing_db_parser_s)

add_turing_test(test_query_ir_compiler_context_reuse CompilerContextReuseTest.cpp)
target_sources(test_query_ir_compiler_context_reuse PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_compiler_context_reuse PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_memory_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_compiler_context_reuse PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_db_pass_pipeline_reuse DBPassPipelineReuseTest.cpp)
target_link_libraries(test_query_ir_db_pass_pipeline_reuse PRIVATE
        turing_db_ir_codegen_s
        turing_db_frontend_cypher_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_ast_s
        turing_db_examples_s)
