# The chunked cross product tests force the product to span chunk boundaries.
add_ir_tests(test_query_ir_cross_product_chunked CrossProductChunkedTest.cpp)

# The chunked probe cases join a side whose rows all carry one key, so the matches of a
# probe chunk outgrow it.
add_ir_tests(test_query_ir_hash_join_chunked_probe HashJoinChunkedProbeTest.cpp)

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_cross_product_cut PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_map_property_join MapPropertyJoinTest.cpp)
target_link_libraries(test_query_ir_map_property_join PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_map_property_join PRIVATE ${IR_TEST_DIR})

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
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_cascaded_merge_join PRIVATE ${IR_TEST_DIR})

# The chunked hash join cases run hand-written db IR through the lowering at several chunk
# sizes against the shared SimpleGraph.
add_ir_tests(test_query_ir_hash_join_chunked HashJoinChunkedTest.cpp)
target_link_libraries(test_query_ir_hash_join_chunked PRIVATE
        turing_db_examples_s
        turing_ir_test_rows_s)

# The probe result cases run hand-written nl IR - one program malformed - against the
# shared SimpleGraph.
add_ir_tests(test_query_ir_hash_join_probe_results HashJoinProbeResultsTest.cpp)
target_link_libraries(test_query_ir_hash_join_probe_results PRIVATE turing_db_examples_s)

# The cost model cases build graphs of their own size and read which form the pipeline
# leaves the cut in, so they need the frontend and the codegen beside the dialects.
add_ir_tests(test_query_ir_hash_join_cost_model HashJoinCostModelTest.cpp)
target_link_libraries(test_query_ir_hash_join_cost_model PRIVATE
        turing_db_ir_codegen_s
        turing_db_frontend_cypher_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_ast_s)

# The embedding key cases build their own graph of vectors and hold the join's rows against
# the cross product and filter it replaces.
add_ir_tests(test_query_ir_hash_join_embedding_key HashJoinEmbeddingKeyTest.cpp)
target_link_libraries(test_query_ir_hash_join_embedding_key PRIVATE turing_ir_test_rows_s)

# The build index cases group rows by key and walk the buckets directly, with no graph or IR.
add_ir_tests(test_query_ir_hash_join_index NLHashJoinIndexTest.cpp)

# The hash join fusion is checked at both IR levels and run against the shared SimpleGraph,
# so it needs the frontend engine beside the dialects.
add_turing_test(test_query_ir_hash_join HashJoinTest.cpp)
target_link_libraries(test_query_ir_hash_join PRIVATE turing_ir_test_hash_join_s)

add_turing_test(test_query_ir_nested_product_filter NestedProductFilterTest.cpp)
target_link_libraries(test_query_ir_nested_product_filter PRIVATE turing_ir_test_hash_join_s)

# The projected-key cases check that a cut still fuses when the query returns the property
# it joins on, so they need the frontend engine beside the dialects.
add_turing_test(test_query_ir_hash_join_projected_key HashJoinProjectedKeyTest.cpp)
target_link_libraries(test_query_ir_hash_join_projected_key PRIVATE turing_ir_test_hash_join_s)

# The comma-pattern join key tests read the rows of a four-pattern MATCH as text, so they
# need StringRowSink beside the v3 engine.
add_turing_test(test_query_ir_comma_pattern_join_keys CommaPatternJoinKeysTest.cpp)
target_sources(test_query_ir_comma_pattern_join_keys PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_comma_pattern_join_keys PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_comma_pattern_join_keys PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_multi_pattern_join MultiPatternJoinTest.cpp)
target_sources(test_query_ir_multi_pattern_join PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_multi_pattern_join PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_multi_pattern_join PRIVATE ${IR_TEST_DIR})
