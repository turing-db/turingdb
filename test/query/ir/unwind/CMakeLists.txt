add_call_v3_test(test_query_ir_unwind_node_seed_v3 UnwindNodeSeedV3Test.cpp)

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
target_include_directories(test_query_ir_unwind_node_seed_codegen PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_cypher_unwind PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_unwind_collect_seed UnwindCollectSeedTest.cpp)
target_link_libraries(test_query_ir_unwind_collect_seed PRIVATE
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
        turing_db_storage_s
        turing_ir_test_rows_s
        MLIRParser)
target_include_directories(test_query_ir_unwind_collect_seed PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_unwind_value_filter_codegen UnwindValueFilterCodegenTest.cpp)
target_link_libraries(test_query_ir_unwind_value_filter_codegen PRIVATE
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
        turing_db_storage_s
        turing_ir_test_rows_s
        MLIRParser)
target_include_directories(test_query_ir_unwind_value_filter_codegen PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_cypher_unwind_expr CypherUnwindExprTest.cpp)
target_link_libraries(test_query_ir_cypher_unwind_expr PRIVATE
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
        turing_ir_test_rows_s
        MLIRParser)
target_include_directories(test_query_ir_cypher_unwind_expr PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_unwind_constant_projection UnwindConstantProjectionTest.cpp)
target_link_libraries(test_query_ir_unwind_constant_projection PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_unwind_constant_projection PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_unwind_heterogeneous_list UnwindHeterogeneousListTest.cpp)
target_sources(test_query_ir_unwind_heterogeneous_list PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_unwind_heterogeneous_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_unwind_heterogeneous_list PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_match_cut_unwind MatchCutUnwindTest.cpp)
target_link_libraries(test_query_ir_match_cut_unwind PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_match_cut_unwind PRIVATE ${IR_TEST_DIR})

# UNWIND of a list whose elements are read per row, run end to end against simpledb.
add_turing_test(test_query_ir_unwind_variables UnwindVariablesTest.cpp)
target_sources(test_query_ir_unwind_variables PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_unwind_variables PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_unwind_variables PRIVATE ${IR_TEST_DIR})

# A CALL subquery and an optional pattern over a column matched against an unwound value.
add_call_v3_test(test_query_ir_unwind_matched_scope UnwindMatchedScopeTest.cpp)

add_turing_test(test_query_ir_unwind_equality_hop UnwindEqualityHopTest.cpp)
target_link_libraries(test_query_ir_unwind_equality_hop PRIVATE
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
        turing_db_ir_exec_s
        turing_db_storage_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_unwind_equality_hop PRIVATE ${IR_TEST_DIR})

add_call_v3_test(test_query_ir_unwind_many_elements UnwindManyElementsTest.cpp)
