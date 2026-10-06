add_ir_tests(test_query_ir_push_down_filter PushDownFilterTest.cpp)

add_ir_tests(test_query_ir_fuse_scan_by_node_ids FuseScanByNodeIDsTest.cpp)

add_ir_tests(test_query_ir_fuse_scan_by_property_value FuseScanByPropertyValueTest.cpp)

add_ir_tests(test_query_ir_trim_unread_columns TrimUnreadColumnsTest.cpp)

add_ir_tests(test_query_ir_trim_call_carry_set TrimCallCarrySetTest.cpp)

add_ir_tests(test_query_ir_reuse_property_reads ReusePropertyReadsTest.cpp)

# The edge-scan fusion tests check the rewritten db program, then lower and run it
# against the shared SimpleGraph fixture to compare its rows with the unfused hop's.
add_ir_tests(test_query_ir_fuse_scan_edges FuseScanEdgesTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_edges PRIVATE turing_db_examples_s)

# The codegen-level cases compile Cypher through the parser, analyzer and DBProgramGenerator
# against the shared SimpleGraph fixture and read the db program the pass pipeline leaves.
add_turing_test(test_query_ir_fuse_scan_edges_codegen FuseScanEdgesCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_edges_codegen PRIVATE
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
target_include_directories(test_query_ir_fuse_scan_edges_codegen PRIVATE ${IR_TEST_DIR})

# The by-type fusion tests check the rewritten db program, then lower and run it against
# the shared SimpleGraph fixture to compare its rows with the plain hop and filter's.
add_ir_tests(test_query_ir_fuse_edges_by_type FuseEdgesByTypeTest.cpp)
target_link_libraries(test_query_ir_fuse_edges_by_type PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_fuse_scan_edges_by_type FuseScanEdgesByTypeTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_edges_by_type PRIVATE turing_db_examples_s)

# The by-label out-edge fusion tests check the rewritten db program, then lower and run it
# against the shared SimpleGraph fixture to compare its rows with the label scan and hop's.
add_ir_tests(test_query_ir_fuse_scan_out_edges_by_label FuseScanOutEdgesByLabelTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_out_edges_by_label PRIVATE turing_db_examples_s)

add_turing_test(test_query_ir_fuse_scan_out_edges_by_label_codegen FuseScanOutEdgesByLabelCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_out_edges_by_label_codegen PRIVATE
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
target_include_directories(test_query_ir_fuse_scan_out_edges_by_label_codegen PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_fuse_scan_in_edges_by_label FuseScanInEdgesByLabelTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_in_edges_by_label PRIVATE turing_db_examples_s)

add_turing_test(test_query_ir_fuse_scan_in_edges_by_label_codegen FuseScanInEdgesByLabelCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_in_edges_by_label_codegen PRIVATE
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
target_include_directories(test_query_ir_fuse_scan_in_edges_by_label_codegen PRIVATE ${IR_TEST_DIR})

# The endpoint-label fusion tests: the same two checks for the scans read off the label set
# of the endpoint the hop arrives at or leaves, rather than the one it starts from.
add_ir_tests(test_query_ir_fuse_scan_edges_by_endpoint_label FuseScanEdgesByEndpointLabelTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_edges_by_endpoint_label PRIVATE turing_db_examples_s)

add_turing_test(test_query_ir_fuse_scan_edges_by_endpoint_label_codegen FuseScanEdgesByEndpointLabelCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_edges_by_endpoint_label_codegen PRIVATE
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
target_include_directories(test_query_ir_fuse_scan_edges_by_endpoint_label_codegen PRIVATE ${IR_TEST_DIR})

# The by-label hop fusion tests: the same two checks for the hops that walk the edges the
# endpoint they reach carries a set of labels on.
add_ir_tests(test_query_ir_fuse_edges_by_endpoint_label FuseEdgesByEndpointLabelTest.cpp)
target_link_libraries(test_query_ir_fuse_edges_by_endpoint_label PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_fuse_edges_by_type_and_label FuseEdgesByTypeAndLabelTest.cpp)
target_link_libraries(test_query_ir_fuse_edges_by_type_and_label PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_remove_redundant_label_checks RemoveRedundantLabelChecksTest.cpp)
target_link_libraries(test_query_ir_remove_redundant_label_checks PRIVATE turing_db_examples_s)

add_turing_test(test_query_ir_remove_redundant_label_checks_codegen RemoveRedundantLabelChecksCodegenTest.cpp)
target_link_libraries(test_query_ir_remove_redundant_label_checks_codegen PRIVATE
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
target_include_directories(test_query_ir_remove_redundant_label_checks_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_fuse_edges_by_endpoint_label_codegen FuseEdgesByEndpointLabelCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_edges_by_endpoint_label_codegen PRIVATE
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
target_include_directories(test_query_ir_fuse_edges_by_endpoint_label_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_fuse_edges_by_type_codegen FuseEdgesByTypeCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_edges_by_type_codegen PRIVATE
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
target_include_directories(test_query_ir_fuse_edges_by_type_codegen PRIVATE ${IR_TEST_DIR})

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
target_include_directories(test_query_ir_fuse_scan_by_node_ids_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_fuse_scan_by_property_value_codegen FuseScanByPropertyValueCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_scan_by_property_value_codegen PRIVATE
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
target_include_directories(test_query_ir_fuse_scan_by_property_value_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_trim_unread_columns_cypher TrimUnreadColumnsCypherTest.cpp)
target_sources(test_query_ir_trim_unread_columns_cypher PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_trim_unread_columns_cypher PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_trim_unread_columns_cypher PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_reuse_property_reads_cypher ReusePropertyReadsCypherTest.cpp)
target_sources(test_query_ir_reuse_property_reads_cypher PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_reuse_property_reads_cypher PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_reuse_property_reads_cypher PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_narrow_edge_type_reads NarrowEdgeTypeReadsTest.cpp)
target_link_libraries(test_query_ir_narrow_edge_type_reads PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_fuse_edge_type_predicates FuseEdgeTypePredicatesTest.cpp)
target_link_libraries(test_query_ir_fuse_edge_type_predicates PRIVATE turing_db_examples_s)

add_ir_tests(test_query_ir_fuse_label_predicates FuseLabelPredicatesTest.cpp)
target_link_libraries(test_query_ir_fuse_label_predicates PRIVATE turing_db_examples_s)

add_turing_test(test_query_ir_fuse_edge_type_predicates_codegen FuseEdgeTypePredicatesCodegenTest.cpp)
target_link_libraries(test_query_ir_fuse_edge_type_predicates_codegen PRIVATE
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
target_include_directories(test_query_ir_fuse_edge_type_predicates_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_narrow_edge_type_reads_codegen NarrowEdgeTypeReadsCodegenTest.cpp)
target_link_libraries(test_query_ir_narrow_edge_type_reads_codegen PRIVATE
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
target_include_directories(test_query_ir_narrow_edge_type_reads_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_narrow_edge_type_reads_query NarrowEdgeTypeReadsQueryTest.cpp)
target_link_libraries(test_query_ir_narrow_edge_type_reads_query PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_narrow_edge_type_reads_query PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_shared_edge_type_check SharedEdgeTypeCheckTest.cpp)

add_ir_tests(test_query_ir_two_filters_over_one_read TwoFiltersOverOneReadTest.cpp)

add_ir_tests(test_query_ir_trim_lazy_case TrimLazyCaseTest.cpp)

add_call_v3_test(test_query_ir_fetch_nodes FetchNodesTest.cpp)

add_ir_tests(test_query_ir_deep_chain_passes DeepChainPassesTest.cpp)

add_ir_tests(test_query_ir_constant_column_deep_chain ConstantColumnDeepChainTest.cpp)

add_call_v3_test(test_query_ir_reroot_id_operand_read_twice RerootIDOperandReadTwiceTest.cpp)

add_call_v3_test(test_query_ir_seed_ids_below_a_filter SeedIDsBelowAFilterTest.cpp)

add_call_v3_test(test_query_ir_reroot_at_scanned_nodes RerootAtScannedNodesTest.cpp)

add_turing_test(test_query_ir_reroot_pattern_at_seed RerootPatternAtSeedTest.cpp)
target_sources(test_query_ir_reroot_pattern_at_seed PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_reroot_pattern_at_seed PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_reroot_pattern_at_seed PRIVATE ${IR_TEST_DIR})
