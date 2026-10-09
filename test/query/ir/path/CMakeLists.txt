add_ir_tests(test_query_ir_explore_paths_dialect ExplorePathsDialectTest.cpp)
target_link_libraries(test_query_ir_explore_paths_dialect PRIVATE turing_ir_test_rows_s turing_db_examples_s)

add_ir_tests(test_query_ir_explore_end_constraint ExploreEndConstraintTest.cpp)
target_link_libraries(test_query_ir_explore_end_constraint PRIVATE turing_ir_test_rows_s turing_db_examples_s)

add_ir_tests(test_query_ir_explore_bound_ends ExploreBoundEndsTest.cpp)
target_link_libraries(test_query_ir_explore_bound_ends PRIVATE turing_ir_test_rows_s turing_db_examples_s)

add_ir_tests(test_query_ir_explore_end_node_set ExploreEndNodeSetTest.cpp)
target_link_libraries(test_query_ir_explore_end_node_set PRIVATE turing_ir_test_rows_s turing_db_examples_s)

add_ir_tests(test_query_ir_explore_end_factor ExploreEndFactorTest.cpp)
target_sources(test_query_ir_explore_end_factor PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_explore_end_factor PRIVATE
        turing_ir_test_rows_s
        turing_db_examples_s
        turing_db_s
        turing_testenv_s
        turing_db_system_s)

add_ir_tests(test_query_ir_explore_end_property ExploreEndPropertyTest.cpp)
target_sources(test_query_ir_explore_end_property PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_explore_end_property PRIVATE
        turing_ir_test_rows_s
        turing_db_examples_s
        turing_db_s
        turing_testenv_s
        turing_db_system_s)

add_ir_tests(test_query_ir_explore_end_set_past_hops ExploreEndSetPastHopsTest.cpp)
target_link_libraries(test_query_ir_explore_end_set_past_hops PRIVATE turing_ir_test_rows_s turing_db_examples_s)

add_call_v3_test(test_query_ir_explore_end_set_past_hops_cypher ExploreEndSetPastHopsCypherTest.cpp)

add_ir_tests(test_query_ir_explore_distinct_ends ExploreDistinctEndsTest.cpp)
target_link_libraries(test_query_ir_explore_distinct_ends PRIVATE turing_ir_test_rows_s turing_db_examples_s)

add_ir_tests(test_query_ir_explore_hop_labels ExploreHopLabelsTest.cpp)
target_link_libraries(test_query_ir_explore_hop_labels PRIVATE turing_ir_test_rows_s turing_db_examples_s)

add_ir_tests(test_query_ir_shortest_path ShortestPathLoweringTest.cpp)
# The path column is rendered to text by StringRowSink.
target_sources(test_query_ir_shortest_path PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)

# The Cypher-level shortest path tests drive SHORTESTPATH end to end through
# QueryInterpreterV3 over a custom weighted graph, reading distance and path as text.
add_turing_test(test_query_ir_cypher_shortest_path CypherShortestPathTest.cpp)
target_sources(test_query_ir_cypher_shortest_path PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_cypher_shortest_path PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_cypher_shortest_path PRIVATE ${IR_TEST_DIR})

# A road whose weight a second change overwrites, so the shortest path is asked which
# datapart it costs the edge from.
add_turing_test(test_query_ir_shortest_path_stale_weight ShortestPathStaleWeightTest.cpp)
target_sources(test_query_ir_shortest_path_stale_weight PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_shortest_path_stale_weight PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_shortest_path_stale_weight PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_shortest_path_uncommitted_weight ShortestPathUncommittedWeightTest.cpp)
target_link_libraries(test_query_ir_shortest_path_uncommitted_weight PRIVATE
        turing_ir_test_write_query_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_shortest_path_uncommitted_weight PRIVATE ${IR_TEST_DIR})

add_call_v3_test(test_query_ir_quantified_path_name_conflict QuantifiedPathNameConflictTest.cpp)

add_call_v3_test(test_query_ir_quantified_self_loop QuantifiedSelfLoopTest.cpp)

add_call_v3_test(test_query_ir_hop_predicate_null_column HopPredicateNullColumnTest.cpp)

add_call_v3_test(test_query_ir_hop_predicate_outer_variable HopPredicateOuterVariableTest.cpp)

add_call_v3_test(test_query_ir_hop_predicate_non_seed_variable HopPredicateNonSeedVariableTest.cpp)

add_call_v3_test(test_query_ir_list_predicate_over_edge_list ListPredicateOverEdgeListTest.cpp)

add_call_v3_test(test_query_ir_list_predicate_over_group ListPredicateOverGroupTest.cpp)

add_call_v3_test(test_query_ir_named_path_cut NamedPathCutTest.cpp)

add_call_v3_test(test_query_ir_collect_variable_length_edges CollectVariableLengthEdgesTest.cpp)

add_call_v3_test(test_query_ir_reverse_seeded_walk ReverseSeededWalkTest.cpp)

add_call_v3_test(test_query_ir_explore_end_set_carry_trim ExploreEndSetCarryTrimTest.cpp)

add_call_v3_test(test_query_ir_explore_paths_cypher ExplorePathsCypherTest.cpp)

add_call_v3_test(test_query_ir_explore_range_literal_cypher ExploreRangeLiteralCypherTest.cpp)

add_call_v3_test(test_query_ir_explore_pattern_spellings_cypher ExplorePatternSpellingsCypherTest.cpp)

add_call_v3_test(test_query_ir_explore_end_constraint_cypher ExploreEndConstraintCypherTest.cpp)

add_call_v3_test(test_query_ir_explore_bound_and_distinct_cypher ExploreBoundAndDistinctCypherTest.cpp)

add_call_v3_test(test_query_ir_explore_edge_type_disjunction ExploreEdgeTypeDisjunctionTest.cpp)

add_call_v3_test(test_query_ir_explore_list_predicate ExploreListPredicateTest.cpp)

add_call_v3_test(test_query_ir_named_path NamedPathTest.cpp)

add_call_v3_test(test_query_ir_named_path_functions NamedPathFunctionsTest.cpp)

add_call_v3_test(test_query_ir_path_functions_of_a_walk PathFunctionsOfAWalkTest.cpp)

add_call_v3_test(test_query_ir_named_path_after_its_filters NamedPathAfterItsFiltersTest.cpp)

add_call_v3_test(test_query_ir_named_path_beside_an_element_where NamedPathBesideAnElementWhereTest.cpp)

add_call_v3_test(test_query_ir_mixed_named_path_seeded_from_its_end MixedNamedPathSeededFromItsEndTest.cpp)

add_call_v3_test(test_query_ir_several_walks SeveralWalksTest.cpp)

add_call_v3_test(test_query_ir_optional_walk OptionalWalkTest.cpp)

add_call_v3_test(test_query_ir_count_path_rows CountPathRowsTest.cpp)

# The k-hop neighbourhood a GraphSAGE layer aggregates over, gathered as one union
# branch per hop.
add_turing_test(test_query_ir_multi_hop_union MultiHopUnionTest.cpp)
target_link_libraries(test_query_ir_multi_hop_union PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_multi_hop_union PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_length_alias LengthAliasTest.cpp)
target_link_libraries(test_query_ir_length_alias PRIVATE
        turing_ir_test_write_query_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_length_alias PRIVATE ${IR_TEST_DIR})

# SHORTESTPATH under a WHERE whose predicate reads both endpoints, run end to end against
# simpledb. The path column is rendered to text by StringRowSink.
add_turing_test(test_query_ir_shortest_path_endpoint_predicate ShortestPathEndpointPredicateTest.cpp)
target_sources(test_query_ir_shortest_path_endpoint_predicate PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_shortest_path_endpoint_predicate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_shortest_path_endpoint_predicate PRIVATE ${IR_TEST_DIR})

add_call_v3_test(test_query_ir_shortest_path_elements ShortestPathElementsTest.cpp)

add_turing_test(test_query_ir_path_element_index PathElementIndexTest.cpp)
target_sources(test_query_ir_path_element_index PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_path_element_index PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_path_element_index PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_path_shapes PathShapesTest.cpp)
target_sources(test_query_ir_path_shapes PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_path_shapes PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_path_shapes PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_reachable_sum ReachableSumTest.cpp)
target_link_libraries(test_query_ir_reachable_sum PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_variable_length_edge_type VariableLengthEdgeTypeTest.cpp)
target_sources(test_query_ir_variable_length_edge_type PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_variable_length_edge_type PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_variable_length_edge_type PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_variable_length_edge_index VariableLengthEdgeIndexTest.cpp)
target_sources(test_query_ir_variable_length_edge_index PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_variable_length_edge_index PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_variable_length_edge_index PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_quantified_group_size QuantifiedGroupSizeTest.cpp)
target_sources(test_query_ir_quantified_group_size PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_quantified_group_size PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_quantified_group_size PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_variable_length_edge_is_null VariableLengthEdgeIsNullTest.cpp)
target_sources(test_query_ir_variable_length_edge_is_null PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_variable_length_edge_is_null PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_variable_length_edge_is_null PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_count_distinct_path CountDistinctPathTest.cpp)
target_sources(test_query_ir_count_distinct_path PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_count_distinct_path PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_count_distinct_path PRIVATE ${IR_TEST_DIR})

add_call_v3_test(test_query_ir_sum_over_path_relationships SumOverPathRelationshipsTest.cpp)

add_call_v3_test(test_query_ir_quantified_body_cypher QuantifiedBodyCypherTest.cpp)

add_call_v3_test(test_query_ir_quantified_body_edge_name QuantifiedBodyEdgeNameTest.cpp)

add_call_v3_test(test_query_ir_quantified_body_node_map_step QuantifiedBodyNodeMapStepTest.cpp)

add_call_v3_test(test_query_ir_quantified_body_map_import QuantifiedBodyMapImportTest.cpp)

add_call_v3_test(test_query_ir_quantified_nested_body QuantifiedNestedBodyTest.cpp)

add_call_v3_test(test_query_ir_quantified_walk_bound_name QuantifiedWalkBoundNameTest.cpp)

add_call_v3_test(test_query_ir_quantified_group_across_scopes QuantifiedGroupAcrossScopesTest.cpp)

add_call_v3_test(test_query_ir_pattern_comprehension_after_walk PatternComprehensionAfterWalkTest.cpp)

add_call_v3_test(test_query_ir_group_in_literal GroupInLiteralTest.cpp)

add_call_v3_test(test_query_ir_match_order_by_group MatchOrderByGroupTest.cpp)

add_call_v3_test(test_query_ir_walk_list_equality WalkListEqualityTest.cpp)

add_call_v3_test(test_query_ir_quantified_group_key QuantifiedGroupKeyTest.cpp)

add_ir_tests(test_query_ir_hoist_hop_conjuncts HoistHopConjunctsTest.cpp)
