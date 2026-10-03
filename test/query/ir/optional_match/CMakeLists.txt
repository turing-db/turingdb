add_turing_test(test_query_ir_optional_match_cut OptionalMatchCutTest.cpp)
target_link_libraries(test_query_ir_optional_match_cut PRIVATE
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
        turing_db_ir_common_s
        MLIRParser)
target_include_directories(test_query_ir_optional_match_cut PRIVATE ${IR_TEST_DIR})

# OPTIONAL MATCH beside the clauses around it: a mandatory MATCH, another OPTIONAL MATCH, a
# WITH barrier, an aggregation and a CALL. Reads its rows as text through StringRowSink,
# since the projections range over IDs, properties and lists.
add_turing_test(test_query_ir_optional_match_composition OptionalMatchCompositionTest.cpp)
target_sources(test_query_ir_optional_match_composition PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_optional_match_composition PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_optional_match_composition PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_optional_match OptionalMatchTest.cpp)
target_link_libraries(test_query_ir_optional_match PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_optional_match PRIVATE ${IR_TEST_DIR})
