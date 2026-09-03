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

# The output-row rendering and the count / row sinks that several of the tests below
# would otherwise each redeclare.
add_library(turing_ir_test_rows_s STATIC IRTestRows.cpp)
target_link_libraries(turing_ir_test_rows_s
    PUBLIC  GTest::gtest
            turing_db_s
            turing_db_storage_s
            turing_db_interpreter_v3_s)
target_include_directories(turing_ir_test_rows_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

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

# The chunked collect tests force the collect fold to span chunk boundaries.
add_ir_tests(test_query_ir_collect_chunked CollectChunkedTest.cpp)

# The collect row-alignment and drain tests run against the shared SimpleGraph fixture.
add_ir_tests(test_query_ir_collect_row_alignment CollectRowAlignmentTest.cpp)
target_link_libraries(test_query_ir_collect_row_alignment PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_collect_drain CollectDrainTest.cpp)
target_link_libraries(test_query_ir_collect_drain PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_collect_result_type CollectResultTypeTest.cpp)

add_ir_tests(test_query_ir_collect_aggregate_kind CollectAggregateKindTest.cpp)

# The CALL procedure tests drive real registered procedures (db.labels) alongside two
# test-local ones, so they need the procedure registry.
add_ir_tests(test_query_ir_call_procedure MLIRCallProcedureTest.cpp)
# The Cypher-level cases run a query through the parser, analyzer and DBProgramGenerator
# against the test's own registry, so they need the frontend as well as the engine.
target_link_libraries(test_query_ir_call_procedure PRIVATE
        turing_db_procedures_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s)

# The three-layer sample test chains three calls of gnn.neighbourhoodSample from the query
# text over a purpose-built citation graph, so it needs the frontend as well as the registry.
add_ir_tests(test_query_ir_gnn_sample_layers GnnSampleLayersTest.cpp)
target_link_libraries(test_query_ir_gnn_sample_layers PRIVATE
        turing_db_procedures_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s)

# The optional-argument tests drive gnn.neighbourhoodSample from hand-written IR - the
# analyzer would reject the wrong-arity forms before they reach the lowering's check -
# so they need only the procedure registry beside the engine.
add_ir_tests(test_query_ir_call_optional_argument CallOptionalArgumentTest.cpp)
target_link_libraries(test_query_ir_call_optional_argument PRIVATE turing_db_procedures_s)

# The drive-loop anchor test calls a test-local procedure whose scalar argument precedes
# the node column from the query text, so it needs the frontend as well as the registry.
add_ir_tests(test_query_ir_call_drive_loop_anchor CallDriveLoopAnchorTest.cpp)
target_link_libraries(test_query_ir_call_drive_loop_anchor PRIVATE
        turing_db_procedures_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s)

# The cross-product argument tests call a test-local procedure reading both factors of
# the product a two-pattern MATCH builds, from the query text, so they need the frontend
# as well as the registry.
add_ir_tests(test_query_ir_call_cross_product_arguments CallCrossProductArgumentsTest.cpp)
target_link_libraries(test_query_ir_call_cross_product_arguments PRIVATE
        turing_db_procedures_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s)

# The yield-into-match tests feed the rows a CALL yielded to a later MATCH - once as a
# pattern variable, once through the MATCH's WHERE - from the query text over the
# simpledb fixture, so they need the frontend, the registry and the examples graph.
add_ir_tests(test_query_ir_call_yield_into_match CallYieldIntoMatchTest.cpp)
target_link_libraries(test_query_ir_call_yield_into_match PRIVATE
        turing_db_procedures_s
        turing_db_examples_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s)

add_ir_tests(test_query_ir_call_yielded_edge_identity CallYieldedEdgeIdentityTest.cpp)
target_link_libraries(test_query_ir_call_yielded_edge_identity PRIVATE
        turing_db_procedures_s
        turing_db_examples_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s)

# The CALL gap tests drive the whole engine through QueryInterpreterV3 on the simpledb
# fixture, writes included, and read their rows as text through StringRowSink.
function(add_call_v3_test exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_sources(${exe_name} PRIVATE StringRowSink.cpp CallV3Test.cpp)
    target_link_libraries(${exe_name} PRIVATE
            turing_db_s
            turing_testenv_s
            turing_db_examples_s
            turing_db_system_s
            turing_db_storage_s
            turing_db_interpreter_v3_s)
    target_include_directories(${exe_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

add_call_v3_test(test_query_ir_call_yielded_property CallYieldedPropertyTest.cpp)
add_call_v3_test(test_query_ir_call_yielded_node_create_edge CallYieldedNodeCreateEdgeTest.cpp)
add_call_v3_test(test_query_ir_call_yielded_entity_delete CallYieldedEntityDeleteTest.cpp)
add_call_v3_test(test_query_ir_call_yielded_sort_key CallYieldedSortKeyTest.cpp)
add_call_v3_test(test_query_ir_call_yielded_aggregate CallYieldedAggregateTest.cpp)
add_call_v3_test(test_query_ir_call_yielded_collect CallYieldedCollectTest.cpp)
add_call_v3_test(test_query_ir_call_yielded_is_null CallYieldedIsNullTest.cpp)
add_call_v3_test(test_query_ir_call_source_create_property CallSourceCreatePropertyTest.cpp)
add_call_v3_test(test_query_ir_call_reserved_word_procedure CallReservedWordProcedureTest.cpp)
add_call_v3_test(test_query_ir_call_yield_star CallYieldStarTest.cpp)
add_call_v3_test(test_query_ir_call_skip_limit CallSkipLimitTest.cpp)
add_call_v3_test(test_query_ir_call_distinct CallDistinctTest.cpp)
add_call_v3_test(test_query_ir_call_return_alias CallReturnAliasTest.cpp)
add_call_v3_test(test_query_ir_call_order_by CallOrderByTest.cpp)
add_call_v3_test(test_query_ir_call_grouped_aggregate CallGroupedAggregateTest.cpp)
add_call_v3_test(test_query_ir_call_unwind CallUnwindTest.cpp)
add_call_v3_test(test_query_ir_call_pattern_shapes CallPatternShapesTest.cpp)
add_call_v3_test(test_query_ir_call_components CallComponentsTest.cpp)
add_call_v3_test(test_query_ir_call_carried_scalar CallCarriedScalarTest.cpp)
add_call_v3_test(test_query_ir_call_writes CallWritesTest.cpp)
add_call_v3_test(test_query_ir_call_boolean_predicates CallBooleanPredicatesTest.cpp)
add_call_v3_test(test_query_ir_call_show_indexes CallShowIndexesTest.cpp)
add_call_v3_test(test_query_ir_scan_by_node_ids_v3 ScanByNodeIDsV3Test.cpp)
add_call_v3_test(test_query_ir_unwind_node_seed_v3 UnwindNodeSeedV3Test.cpp)

# The crossing-call tests hop twice from the column a CALL yielded and then call again,
# once with a constant argument alone and once reading the chain's tail, from the query
# text over the simpledb fixture - so they need the frontend, the registry and the
# examples graph.
add_ir_tests(test_query_ir_crossing_call CrossingCallTest.cpp)
target_link_libraries(test_query_ir_crossing_call PRIVATE
        turing_db_procedures_s
        turing_db_examples_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s)

# The db.* procedure tests drive the registered schema and version-control procedures from
# the query text alone, so they need the procedure registry and the frontend as well as the
# engine.
add_ir_tests(test_query_ir_db_procedures MLIRDBProceduresTest.cpp)
target_link_libraries(test_query_ir_db_procedures PRIVATE
        turing_db_procedures_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s)

# The Cypher-level collect tests drive queries end to end through QueryInterpreterV3.
add_ir_tests(test_query_ir_cypher_collect CypherCollectTest.cpp)
target_link_libraries(test_query_ir_cypher_collect PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_interpreter_v3_s)

add_ir_tests(test_query_ir_cypher_collect_distinct_entity CypherCollectDistinctEntityTest.cpp)
target_link_libraries(test_query_ir_cypher_collect_distinct_entity PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_interpreter_v3_s)

add_ir_tests(test_query_ir_cypher_collect_element_type CypherCollectElementTypeTest.cpp)
target_link_libraries(test_query_ir_cypher_collect_element_type PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_interpreter_v3_s)

add_ir_tests(test_query_ir_cypher_collect_aggregate CypherCollectAggregateTest.cpp)
target_link_libraries(test_query_ir_cypher_collect_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_interpreter_v3_s)

add_ir_tests(test_query_ir_cypher_collect_input CypherCollectInputTest.cpp)
target_link_libraries(test_query_ir_cypher_collect_input PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_interpreter_v3_s)

add_ir_tests(test_query_ir_dbdialect DBDialectTest.cpp)
add_ir_tests(test_query_ir_push_down_filter PushDownFilterTest.cpp)
add_ir_tests(test_query_ir_fuse_scan_by_node_ids FuseScanByNodeIDsTest.cpp)

# The codegen-level cases compile Cypher through the parser, analyzer and DBProgramGenerator
# against the shared SimpleGraph fixture and read the db program the pass pipeline leaves.
add_turing_test(test_query_ir_fuse_scan_by_node_ids_codegen FuseScanByNodeIDsCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_by_node_ids_codegen PRIVATE
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
        turing_db_storage_s)
target_include_directories(test_query_ir_fuse_scan_by_node_ids_codegen PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# The db program an UNWIND seeding a pattern node compiles to, read against the shared
# SimpleGraph fixture the same way.
add_turing_test(test_query_ir_unwind_node_seed_codegen UnwindNodeSeedCodegenTest.cpp)
target_link_libraries(test_query_ir_unwind_node_seed_codegen PRIVATE
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
        turing_db_storage_s)
target_include_directories(test_query_ir_unwind_node_seed_codegen PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_ir_tests(test_query_ir_nldialect NLDialectTest.cpp)

# The keyless collect drain emits its one group whether or not a row arrived, against the
# shared SimpleGraph fixture and an empty graph.
add_ir_tests(test_query_ir_keyless_collect_empty_input KeylessCollectEmptyInputTest.cpp)
target_link_libraries(test_query_ir_keyless_collect_empty_input PRIVATE turing_db_examples_s)

# The reduced-row output tests read the single row a reduction collapsed to from another
# block, against the shared SimpleGraph fixture.
add_ir_tests(test_query_ir_reduced_row_output ReducedRowOutputTest.cpp)
target_link_libraries(test_query_ir_reduced_row_output PRIVATE turing_db_examples_s)

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
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
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

add_turing_test(test_query_ir_interpreter_v3_call QueryInterpreterV3CallTest.cpp)
target_link_libraries(test_query_ir_interpreter_v3_call PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_interpreter_v3_call PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_system_command_equivalence SystemCommandEquivalenceTest.cpp)
target_link_libraries(test_query_ir_system_command_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_system_command_equivalence PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_system_command PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# VECTOR SEARCH runs against the session's vector indexes, which the system manager
# holds, so the search tests drive the whole engine the way the system-command ones do.
add_turing_test(test_query_ir_vector_search VectorSearchTest.cpp)
# The traversal cases read their rows as text, whatever kinds the projected columns are.
target_sources(test_query_ir_vector_search PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_vector_search PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# The composition cases drive the search through a WITH, a CALL and an aggregate, so they
# need the procedure-driving engine the same way the search tests do.
add_turing_test(test_query_ir_vector_search_composition VectorSearchCompositionTest.cpp)
target_sources(test_query_ir_vector_search_composition PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search_composition PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_vector_search_composition PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# The metric / index-kind cases build one index per case, so they drive the whole engine
# the way the other search tests do.
add_turing_test(test_query_ir_vector_search_index_kind VectorSearchIndexKindTest.cpp)
target_sources(test_query_ir_vector_search_index_kind PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search_index_kind PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_vector_search_index_kind PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# The write cases run their statements inside a change and submit it, so they need the
# change management the system manager holds.
add_turing_test(test_query_ir_vector_search_write VectorSearchWriteTest.cpp)
target_sources(test_query_ir_vector_search_write PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search_write PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_vector_search_write PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_vector_search_rejection VectorSearchRejectionTest.cpp)
target_sources(test_query_ir_vector_search_rejection PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search_rejection PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_vector_search_rejection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_call_yield_group_aggregate CallYieldGroupAggregateTest.cpp)
target_link_libraries(test_query_ir_call_yield_group_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_call_yield_group_aggregate PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_call_yield_count_distinct CallYieldCountDistinctTest.cpp)
target_link_libraries(test_query_ir_call_yield_count_distinct PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_call_yield_count_distinct PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_set_equivalence PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_delete_equivalence DeleteEquivalenceTest.cpp)
target_link_libraries(test_query_ir_delete_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
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

add_turing_test(test_query_ir_order_by_labels OrderByLabelsTest.cpp)
target_link_libraries(test_query_ir_order_by_labels PRIVATE
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
target_include_directories(test_query_ir_order_by_labels PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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

add_turing_test(test_query_ir_string_concat StringConcatTest.cpp)
target_link_libraries(test_query_ir_string_concat PRIVATE
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
target_include_directories(test_query_ir_string_concat PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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

add_turing_test(test_query_ir_cypher_list_literal CypherListLiteralTest.cpp)
target_link_libraries(test_query_ir_cypher_list_literal PRIVATE
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
target_include_directories(test_query_ir_cypher_list_literal PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_list_literal_element_type ListLiteralElementTypeTest.cpp)
target_link_libraries(test_query_ir_list_literal_element_type PRIVATE
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
target_include_directories(test_query_ir_list_literal_element_type PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_list_literal_property_value ListLiteralPropertyValueTest.cpp)
target_link_libraries(test_query_ir_list_literal_property_value PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_storage_s)
target_include_directories(test_query_ir_list_literal_property_value PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_aggregate_over_constant_alias PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with WithTest.cpp)
target_link_libraries(test_query_ir_with PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_ldbc_bi11 LdbcBi11Test.cpp)
target_link_libraries(test_query_ir_ldbc_bi11 PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_ldbc_bi11 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with_multi_part WithMultiPartTest.cpp)
target_link_libraries(test_query_ir_with_multi_part PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with_multi_part PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with_aggregate WithAggregateTest.cpp)
target_link_libraries(test_query_ir_with_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with_aggregate PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with_cut WithCutTest.cpp)
target_link_libraries(test_query_ir_with_cut PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with_cut PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_constant_return_after_with_limit ConstantReturnAfterWithLimitTest.cpp)
target_link_libraries(test_query_ir_constant_return_after_with_limit PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_constant_return_after_with_limit PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with_collect WithCollectTest.cpp)
target_link_libraries(test_query_ir_with_collect PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with_collect PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_cypher_multi_collect CypherMultiCollectTest.cpp)
target_link_libraries(test_query_ir_cypher_multi_collect PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_cypher_multi_collect PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with_scope WithScopeTest.cpp)
target_link_libraries(test_query_ir_with_scope PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with_scope PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_create_return CreateReturnTest.cpp)
target_link_libraries(test_query_ir_create_return PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_create_return PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_with_write WithWriteTest.cpp)
target_link_libraries(test_query_ir_with_write PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with_write PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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

# The crossed-source constant tests run a query whose constant is in flight when a search
# or a CALL opens a product, and read their rows as text.
add_turing_test(test_query_ir_crossed_source_constant CrossedSourceConstantTest.cpp)
target_link_libraries(test_query_ir_crossed_source_constant PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_crossed_source_constant PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

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

add_turing_test(test_query_ir_cascaded_merge_join CascadedMergeJoinTest.cpp)
target_link_libraries(test_query_ir_cascaded_merge_join PRIVATE
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
target_include_directories(test_query_ir_cascaded_merge_join PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_aggregate_over_constant_arithmetic AggregateOverConstantArithmeticTest.cpp)
target_link_libraries(test_query_ir_aggregate_over_constant_arithmetic PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_aggregate_over_constant_arithmetic PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_count_wildcard_empty_input CountWildcardEmptyInputTest.cpp)
target_link_libraries(test_query_ir_count_wildcard_empty_input PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_count_wildcard_empty_input PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_repeated_edge_variable RepeatedEdgeVariableTest.cpp)
target_link_libraries(test_query_ir_repeated_edge_variable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_repeated_edge_variable PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# The comma-pattern join key tests read the rows of a four-pattern MATCH as text, so they
# need StringRowSink beside the v3 engine.
add_turing_test(test_query_ir_comma_pattern_join_keys CommaPatternJoinKeysTest.cpp)
target_sources(test_query_ir_comma_pattern_join_keys PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_comma_pattern_join_keys PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_comma_pattern_join_keys PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_variable_type_conflict VariableTypeConflictTest.cpp)
target_link_libraries(test_query_ir_variable_type_conflict PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_variable_type_conflict PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_unknown_variable UnknownVariableTest.cpp)
target_link_libraries(test_query_ir_unknown_variable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_unknown_variable PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_aggregate_with_return_items AggregateWithReturnItemsTest.cpp)
target_sources(test_query_ir_aggregate_with_return_items PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_aggregate_with_return_items PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_aggregate_with_return_items PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_string_predicates StringPredicateTest.cpp)
target_sources(test_query_ir_string_predicates PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_string_predicates PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_string_predicates PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_pattern_loop PatternLoopTest.cpp)
target_sources(test_query_ir_pattern_loop PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_pattern_loop PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_pattern_loop PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_multi_pattern_join MultiPatternJoinTest.cpp)
target_sources(test_query_ir_multi_pattern_join PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_multi_pattern_join PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_multi_pattern_join PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_match_order_by MatchOrderByTest.cpp)
target_sources(test_query_ir_match_order_by PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_match_order_by PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_match_order_by PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_match_skip_limit MatchSkipLimitTest.cpp)
target_sources(test_query_ir_match_skip_limit PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_match_skip_limit PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_match_skip_limit PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_return_heterogeneous_list ReturnHeterogeneousListTest.cpp)
target_link_libraries(test_query_ir_return_heterogeneous_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_return_heterogeneous_list PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_unwind_heterogeneous_list UnwindHeterogeneousListTest.cpp)
target_sources(test_query_ir_unwind_heterogeneous_list PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_unwind_heterogeneous_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_unwind_heterogeneous_list PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_avg_unwind AvgUnwindTest.cpp)
target_sources(test_query_ir_avg_unwind PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_avg_unwind PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_avg_unwind PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_avg_empty AvgEmptyTest.cpp)
target_sources(test_query_ir_avg_empty PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_avg_empty PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_avg_empty PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_avg_literal AvgLiteralTest.cpp)
target_sources(test_query_ir_avg_literal PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_avg_literal PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_avg_literal PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_multi_match MultiMatchTest.cpp)
target_link_libraries(test_query_ir_multi_match PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_multi_match PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_query_ir_anonymous_match AnonymousMatchTest.cpp)
target_sources(test_query_ir_anonymous_match PRIVATE StringRowSink.cpp)
target_link_libraries(test_query_ir_anonymous_match PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_interpreter_v3_s)
target_include_directories(test_query_ir_anonymous_match PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
