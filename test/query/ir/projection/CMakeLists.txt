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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_order_by PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_order_by_labels PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_order_by_alias_key PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_order_by_constant_alias_key PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_constant_alias_expr_column PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_order_by_count PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_constant_output PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_distinct PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_distinct_order_by_property DistinctOrderByPropertyTest.cpp)
target_link_libraries(test_query_ir_distinct_order_by_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_distinct_order_by_property PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_limit_skip PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_wildcard_return PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_constant_only_projection ConstantOnlyProjectionTest.cpp)
target_link_libraries(test_query_ir_constant_only_projection PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_constant_only_projection PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_constant_return_after_with_limit ConstantReturnAfterWithLimitTest.cpp)
target_link_libraries(test_query_ir_constant_return_after_with_limit PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_constant_return_after_with_limit PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_order_by_aggregate_unprojected_key PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_constant_predicate_projection ConstantPredicateProjectionTest.cpp)
target_link_libraries(test_query_ir_constant_predicate_projection PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_constant_predicate_projection PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_match_order_by MatchOrderByTest.cpp)
target_sources(test_query_ir_match_order_by PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_match_order_by PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_match_order_by PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_match_skip_limit MatchSkipLimitTest.cpp)
target_sources(test_query_ir_match_skip_limit PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_match_skip_limit PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_match_skip_limit PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_order_by_aggregate_key OrderByAggregateKeyTest.cpp)
target_link_libraries(test_query_ir_order_by_aggregate_key PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_order_by_aggregate_key PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_bool_ordering BoolOrderingTest.cpp)
target_link_libraries(test_query_ir_bool_ordering PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_bool_ordering PRIVATE ${IR_TEST_DIR})
