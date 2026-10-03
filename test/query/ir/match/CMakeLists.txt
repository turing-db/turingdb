add_call_v3_test(test_query_ir_index_pattern_inline_where IndexPatternInlineWhereTest.cpp)

add_call_v3_test(test_query_ir_scan_by_node_ids_v3 ScanByNodeIDsV3Test.cpp)

add_call_v3_test(test_query_ir_scan_by_property_value_v3 ScanByPropertyValueV3Test.cpp)

add_call_v3_test(test_query_ir_self_referencing_pattern SelfReferencingPatternTest.cpp)

# The literal kinds a property scan resolves, including the f64 pairing the analyzer keeps
# out of Cypher's reach, run end to end from written db IR.
add_ir_tests(test_query_ir_scan_by_property_value_lowering ScanByPropertyValueLoweringTest.cpp)

# The branch two-hop test runs against the shared SimpleGraph fixture.
add_ir_tests(test_query_ir_branch_two_hop BranchTwoHopTest.cpp)
target_link_libraries(test_query_ir_branch_two_hop PRIVATE turing_db_examples_s)

add_turing_test(test_query_ir_repeated_edge_variable RepeatedEdgeVariableTest.cpp)
target_link_libraries(test_query_ir_repeated_edge_variable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_repeated_edge_variable PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_pattern_loop PatternLoopTest.cpp)
target_sources(test_query_ir_pattern_loop PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_pattern_loop PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_pattern_loop PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_multi_match MultiMatchTest.cpp)
target_link_libraries(test_query_ir_multi_match PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_anonymous_match AnonymousMatchTest.cpp)
target_sources(test_query_ir_anonymous_match PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_anonymous_match PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_anonymous_match PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_anonymous_pattern_property AnonymousPatternPropertyTest.cpp)
target_sources(test_query_ir_anonymous_pattern_property PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_anonymous_pattern_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_anonymous_pattern_property PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_label_predicate LabelPredicateTest.cpp)
target_link_libraries(test_query_ir_label_predicate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_label_predicate PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_label_predicate_codegen LabelPredicateCodegenTest.cpp)
target_link_libraries(test_query_ir_label_predicate_codegen PRIVATE
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
target_include_directories(test_query_ir_label_predicate_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_label_list_chunk LabelListChunkTest.cpp)
target_link_libraries(test_query_ir_label_list_chunk PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_label_list_chunk PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_edge_type_chunk EdgeTypeChunkTest.cpp)
target_link_libraries(test_query_ir_edge_type_chunk PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_edge_type_chunk PRIVATE ${IR_TEST_DIR})

# A pattern closing a cycle on an undirected hop, run end to end against simpledb.
add_call_v3_test(test_query_ir_undirected_cycle UndirectedCycleTest.cpp)

# A hop reaching the head of a cycle before the cycle closes, run end to end against simpledb.
add_call_v3_test(test_query_ir_cycle_head_hop CycleHeadHopTest.cpp)

# Patterns closing cycles of every shape, each checked against its rewrite with no cycle.
add_call_v3_test(test_query_ir_cycle_shapes CycleShapesTest.cpp)

# Ordering the string type() answers against another string, run end to end against simpledb.
add_turing_test(test_query_ir_edge_type_ordering EdgeTypeOrderingTest.cpp)
target_sources(test_query_ir_edge_type_ordering PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_edge_type_ordering PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_edge_type_ordering PRIVATE ${IR_TEST_DIR})

# min() and max() over the string type() answers, run end to end against simpledb.
add_turing_test(test_query_ir_edge_type_reduction EdgeTypeReductionTest.cpp)
target_sources(test_query_ir_edge_type_reduction PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_edge_type_reduction PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_edge_type_reduction PRIVATE ${IR_TEST_DIR})

# startNode() and endNode() over the edges of simpledb, run end to end.
add_turing_test(test_query_ir_edge_endpoints EdgeEndpointsTest.cpp)
target_link_libraries(test_query_ir_edge_endpoints PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_edge_endpoints PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_edge_type_disjunction EdgeTypeDisjunctionTest.cpp)
target_link_libraries(test_query_ir_edge_type_disjunction PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_edge_type_disjunction PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_component_name_resolution ComponentNameResolutionTest.cpp)
target_link_libraries(test_query_ir_component_name_resolution PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_component_name_resolution PRIVATE ${IR_TEST_DIR})

# Two dashes as a subtraction of a negation, or as an undirected hop between nodes.
add_call_v3_test(test_query_ir_double_dash DoubleDashTest.cpp)
