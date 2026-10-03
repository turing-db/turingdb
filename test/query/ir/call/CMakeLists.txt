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

add_call_v3_test(test_query_ir_call_yielded_property CallYieldedPropertyTest.cpp)

add_call_v3_test(test_query_ir_call_yielded_node_create_edge CallYieldedNodeCreateEdgeTest.cpp)

add_call_v3_test(test_query_ir_call_yielded_entity_delete CallYieldedEntityDeleteTest.cpp)

add_call_v3_test(test_query_ir_call_yielded_sort_key CallYieldedSortKeyTest.cpp)

add_call_v3_test(test_query_ir_call_yielded_aggregate CallYieldedAggregateTest.cpp)

add_call_v3_test(test_query_ir_call_yielded_collect CallYieldedCollectTest.cpp)

add_call_v3_test(test_query_ir_call_yielded_is_null CallYieldedIsNullTest.cpp)

add_call_v3_test(test_query_ir_call_source_create_property CallSourceCreatePropertyTest.cpp)

add_call_v3_test(test_query_ir_call_reserved_word_procedure CallReservedWordProcedureTest.cpp)

add_call_v3_test(test_query_ir_call_graphsage CallGraphSAGETest.cpp)

add_call_v3_test(test_query_ir_call_yield_star CallYieldStarTest.cpp)

add_call_v3_test(test_query_ir_call_skip_limit CallSkipLimitTest.cpp)

add_call_v3_test(test_query_ir_call_distinct CallDistinctTest.cpp)

add_call_v3_test(test_query_ir_call_return_alias CallReturnAliasTest.cpp)

add_call_v3_test(test_query_ir_call_order_by CallOrderByTest.cpp)

add_call_v3_test(test_query_ir_call_grouped_aggregate CallGroupedAggregateTest.cpp)

add_call_v3_test(test_query_ir_call_unwind CallUnwindTest.cpp)

add_call_v3_test(test_query_ir_call_pattern_shapes CallPatternShapesTest.cpp)

add_call_v3_test(test_query_ir_call_undirected_row_count CallUndirectedRowCountTest.cpp)

add_call_v3_test(test_query_ir_call_components CallComponentsTest.cpp)

add_call_v3_test(test_query_ir_call_carried_scalar CallCarriedScalarTest.cpp)

add_call_v3_test(test_query_ir_call_writes CallWritesTest.cpp)

add_call_v3_test(test_query_ir_call_boolean_predicates CallBooleanPredicatesTest.cpp)

add_call_v3_test(test_query_ir_call_show_indexes CallShowIndexesTest.cpp)

add_call_v3_test(test_query_ir_standalone_call StandaloneCallTest.cpp)

add_call_v3_test(test_query_ir_unwind_node_seed_call_v3 UnwindNodeSeedCallV3Test.cpp)

add_call_v3_test(test_query_ir_gnn_frontier_union GnnFrontierUnionTest.cpp)

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

# The db program a CALL feeding a MATCH compiles to when the pattern holds a second
# component, read against the shared SimpleGraph fixture the same way.
add_turing_test(test_query_ir_call_driven_island_codegen CallDrivenIslandCodegenTest.cpp)
target_link_libraries(test_query_ir_call_driven_island_codegen PRIVATE
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
target_include_directories(test_query_ir_call_driven_island_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_exec_v3_call QueryInterpreterV3CallTest.cpp)
target_link_libraries(test_query_ir_exec_v3_call PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_exec_v3_call PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_call_yield_group_aggregate CallYieldGroupAggregateTest.cpp)
target_link_libraries(test_query_ir_call_yield_group_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_call_yield_group_aggregate PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_call_yield_count_distinct CallYieldCountDistinctTest.cpp)
target_link_libraries(test_query_ir_call_yield_count_distinct PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_call_yield_count_distinct PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_chained_call PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_call_nullable_yield CallNullableYieldTest.cpp)
target_link_libraries(test_query_ir_call_nullable_yield PRIVATE
        turing_db_procedures_s
        turing_db_examples_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_call_nullable_yield PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_call_yielded_bool CallYieldedBoolTest.cpp)
target_link_libraries(test_query_ir_call_yielded_bool PRIVATE
        turing_db_procedures_s
        turing_db_examples_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_call_yielded_bool PRIVATE ${IR_TEST_DIR})

# A standalone CALL as a branch of a UNION.
add_call_v3_test(test_query_ir_union_standalone_call UnionStandaloneCallTest.cpp)
