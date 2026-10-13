add_ir_tests(test_query_ir_nlexecutor NLExecutorTest.cpp)

add_ir_tests(test_query_ir_dblowering DBLoweringTest.cpp)
# The nested cross product execution tests run against the shared SimpleGraph fixture.

add_call_v3_test(test_query_ir_empty_result_column_names EmptyResultColumnNamesTest.cpp)

add_ir_tests(test_query_ir_dbdialect DBDialectTest.cpp)

add_ir_tests(test_query_ir_nldialect NLDialectTest.cpp)

add_ir_tests(test_query_ir_kernel_result_nullability KernelResultNullabilityTest.cpp)

# The reduced-row output tests read the single row a reduction collapsed to from another
# block, against the shared SimpleGraph fixture.
add_ir_tests(test_query_ir_reduced_row_output ReducedRowOutputTest.cpp)

add_turing_test(test_query_ir_where_expr_registration WhereExprRegistrationTest.cpp)
target_include_directories(test_query_ir_where_expr_registration PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_exec_v3_error QueryInterpreterV3ErrorTest.cpp)
target_include_directories(test_query_ir_exec_v3_error PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_equivalence EquivalenceTest.cpp)
target_include_directories(test_query_ir_equivalence PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_cypher_output CypherOutputTest.cpp)
target_include_directories(test_query_ir_cypher_output PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_output_column_names OutputColumnNamesTest.cpp)
target_include_directories(test_query_ir_output_column_names PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_variable_type_conflict VariableTypeConflictTest.cpp)
target_include_directories(test_query_ir_variable_type_conflict PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_unknown_variable UnknownVariableTest.cpp)
target_include_directories(test_query_ir_unknown_variable PRIVATE ${IR_TEST_DIR})

# The EXPLAIN dumps run the whole compilation of a query against the shared SimpleGraph
# fixture, so the test needs the db dialect - whose passes the prefix reports on -
# alongside the engine.
add_ir_tests(test_query_ir_explain ExplainTest.cpp)
target_sources(test_query_ir_explain PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_explain PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_complex_composition ComplexCompositionTest.cpp)
target_include_directories(test_query_ir_complex_composition PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_mask_column_carry MaskColumnCarryTest.cpp)
target_include_directories(test_query_ir_mask_column_carry PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_signed_number_lexing SignedNumberLexingTest.cpp)
target_include_directories(test_query_ir_signed_number_lexing PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_keyword_named_identifier KeywordNamedIdentifierTest.cpp)
target_sources(test_query_ir_keyword_named_identifier PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_keyword_named_identifier PRIVATE ${IR_TEST_DIR})

# Expressions nested up to and past the parser's maximum depth.
add_call_v3_test(test_query_ir_deep_expression DeepExpressionTest.cpp)

add_turing_test(test_query_ir_compiler_context_reuse CompilerContextReuseTest.cpp)
target_sources(test_query_ir_compiler_context_reuse PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_compiler_context_reuse PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_db_pass_pipeline_reuse DBPassPipelineReuseTest.cpp)
