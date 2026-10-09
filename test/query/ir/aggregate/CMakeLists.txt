add_ir_tests(test_query_ir_group_aggregate GroupAggregateTest.cpp)
target_link_libraries(test_query_ir_group_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_ir_exec_s)

add_ir_tests(test_query_ir_collect CollectTest.cpp)

# A grouping key computed from constants alone, which the lowering has to lay out over
# the rows it stands for before the group assignment reads it.
add_ir_tests(test_query_ir_constant_grouping_key ConstantGroupingKeyTest.cpp)

# The same shape as the reported Cypher queries, end to end on the v3 engine.
add_turing_test(test_query_ir_constant_grouping_key_query ConstantGroupingKeyQueryTest.cpp)
target_sources(test_query_ir_constant_grouping_key_query PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_constant_grouping_key_query PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_constant_grouping_key_query PRIVATE ${IR_TEST_DIR})

# The chunked collect tests force the collect fold to span chunk boundaries.
add_ir_tests(test_query_ir_collect_chunked CollectChunkedTest.cpp)

# The collect row-alignment and drain tests run against the shared SimpleGraph fixture.
add_ir_tests(test_query_ir_collect_row_alignment CollectRowAlignmentTest.cpp)
target_link_libraries(test_query_ir_collect_row_alignment PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_collect_drain CollectDrainTest.cpp)
target_link_libraries(test_query_ir_collect_drain PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_collect_result_type CollectResultTypeTest.cpp)

add_ir_tests(test_query_ir_collect_aggregate_kind CollectAggregateKindTest.cpp)

add_call_v3_test(test_query_ir_collect_concatenation CollectConcatenationTest.cpp)

# The Cypher-level collect tests drive queries end to end through QueryInterpreterV3.
add_ir_tests(test_query_ir_cypher_collect CypherCollectTest.cpp)
target_link_libraries(test_query_ir_cypher_collect PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_ir_exec_s)

add_ir_tests(test_query_ir_cypher_collect_distinct_entity CypherCollectDistinctEntityTest.cpp)
target_link_libraries(test_query_ir_cypher_collect_distinct_entity PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_ir_exec_s)

add_ir_tests(test_query_ir_cypher_collect_element_type CypherCollectElementTypeTest.cpp)
target_link_libraries(test_query_ir_cypher_collect_element_type PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_ir_exec_s)

add_ir_tests(test_query_ir_cypher_collect_aggregate CypherCollectAggregateTest.cpp)
target_link_libraries(test_query_ir_cypher_collect_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_ir_exec_s)

add_ir_tests(test_query_ir_cypher_collect_input CypherCollectInputTest.cpp)
target_link_libraries(test_query_ir_cypher_collect_input PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_ir_exec_s)

add_ir_tests(test_query_ir_count_from_metadata CountFromMetadataTest.cpp)

# The keyless collect drain emits its one group whether or not a row arrived, against the
# shared SimpleGraph fixture and an empty graph.
add_ir_tests(test_query_ir_keyless_collect_empty_input KeylessCollectEmptyInputTest.cpp)
target_link_libraries(test_query_ir_keyless_collect_empty_input PRIVATE turing_db_examples_s)

add_turing_test(test_query_ir_list_index_aggregate ListIndexAggregateTest.cpp)
target_link_libraries(test_query_ir_list_index_aggregate PRIVATE
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
target_include_directories(test_query_ir_list_index_aggregate PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_group_aggregate_arithmetic PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_group_aggregate_expression PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_count_distinct PRIVATE ${IR_TEST_DIR})

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
target_include_directories(test_query_ir_nested_aggregate_projection PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_aggregate_over_constant_alias AggregateOverConstantAliasTest.cpp)
target_link_libraries(test_query_ir_aggregate_over_constant_alias PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_aggregate_over_constant_alias PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_with_aggregate WithAggregateTest.cpp)
target_link_libraries(test_query_ir_with_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with_aggregate PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_with_collect WithCollectTest.cpp)
target_link_libraries(test_query_ir_with_collect PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_with_collect PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_cypher_multi_collect CypherMultiCollectTest.cpp)
target_link_libraries(test_query_ir_cypher_multi_collect PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_cypher_multi_collect PRIVATE ${IR_TEST_DIR})

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
target_include_directories(test_query_ir_count_wildcard_input PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_count_distinct_ordering PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_count_distinct_argument PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_count_distinct_grouping PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_count_distinct_value PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_aggregate_over_constant_arithmetic AggregateOverConstantArithmeticTest.cpp)
target_link_libraries(test_query_ir_aggregate_over_constant_arithmetic PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_aggregate_over_constant_arithmetic PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_count_wildcard_empty_input CountWildcardEmptyInputTest.cpp)
target_link_libraries(test_query_ir_count_wildcard_empty_input PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_count_wildcard_empty_input PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_count_from_metadata_query CountFromMetadataQueryTest.cpp)
target_link_libraries(test_query_ir_count_from_metadata_query PRIVATE
        turing_ir_test_write_query_s
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)

add_turing_test(test_query_ir_aggregate_with_return_items AggregateWithReturnItemsTest.cpp)
target_sources(test_query_ir_aggregate_with_return_items PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_aggregate_with_return_items PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_aggregate_with_return_items PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_avg_unwind AvgUnwindTest.cpp)
target_sources(test_query_ir_avg_unwind PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_avg_unwind PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_avg_unwind PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_avg_empty AvgEmptyTest.cpp)
target_sources(test_query_ir_avg_empty PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_avg_empty PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_avg_empty PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_avg_literal AvgLiteralTest.cpp)
target_sources(test_query_ir_avg_literal PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_avg_literal PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_avg_literal PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_count_alias CountAliasTest.cpp)
target_sources(test_query_ir_count_alias PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_count_alias PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_count_alias PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_grouping_key ListGroupingKeyTest.cpp)
target_link_libraries(test_query_ir_list_grouping_key PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_grouping_key PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_min_max_orderable MinMaxOrderableTest.cpp)
target_link_libraries(test_query_ir_min_max_orderable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_min_max_orderable PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_min_max_over_lists MinMaxOverListsTest.cpp)
target_link_libraries(test_query_ir_min_max_over_lists PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_min_max_over_lists PRIVATE ${IR_TEST_DIR})

# Reductions over a column that is null on every row, run end to end over SimpleGraph.
add_turing_test(test_query_ir_null_aggregate NullAggregateTest.cpp)
target_link_libraries(test_query_ir_null_aggregate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_null_aggregate PRIVATE ${IR_TEST_DIR})

# A node or an edge indexed out of a collected list, run end to end against simpledb.
add_turing_test(test_query_ir_collected_entity_index CollectedEntityIndexTest.cpp)
target_sources(test_query_ir_collected_entity_index PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_collected_entity_index PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_collected_entity_index PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_null_column_aggregate_pair NullColumnAggregatePairTest.cpp)
target_link_libraries(test_query_ir_null_column_aggregate_pair PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_null_column_aggregate_pair PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_min_max_over_nullable MinMaxOverNullableTest.cpp)
target_link_libraries(test_query_ir_min_max_over_nullable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_min_max_over_nullable PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_integer_sum_past_null IntegerSumPastNullTest.cpp)
target_link_libraries(test_query_ir_integer_sum_past_null PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_integer_sum_past_null PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_keyless_aggregates_over_element_ops KeylessAggregatesOverElementOpsTest.cpp)
target_sources(test_query_ir_keyless_aggregates_over_element_ops PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_keyless_aggregates_over_element_ops PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_keyless_aggregates_over_element_ops PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_comprehension_aggregate_order_by ListComprehensionAggregateOrderByTest.cpp)
target_link_libraries(test_query_ir_list_comprehension_aggregate_order_by PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_comprehension_aggregate_order_by PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_count_comparison CountComparisonTest.cpp)
target_link_libraries(test_query_ir_count_comparison PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_component_of_grouping_key ComponentOfGroupingKeyTest.cpp)
target_link_libraries(test_query_ir_component_of_grouping_key PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_component_of_grouping_key PRIVATE ${IR_TEST_DIR})

# collect() over toString() results.
add_call_v3_test(test_query_ir_collect_computed_string CollectComputedStringTest.cpp)

add_turing_test(test_query_ir_count_distinct_case CountDistinctCaseTest.cpp)
target_link_libraries(test_query_ir_count_distinct_case PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_standard_deviation StandardDeviationTest.cpp)
target_sources(test_query_ir_standard_deviation PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_standard_deviation PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_standard_deviation PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_percentile PercentileTest.cpp)
target_sources(test_query_ir_percentile PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_percentile PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_percentile PRIVATE ${IR_TEST_DIR})
