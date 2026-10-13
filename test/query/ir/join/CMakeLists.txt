# The chunked cross product tests force the product to span chunk boundaries.
add_ir_tests(test_query_ir_cross_product_chunked CrossProductChunkedTest.cpp)

# The chunked probe cases join a side whose rows all carry one key, so the matches of a
# probe chunk outgrow it.
add_ir_tests(test_query_ir_hash_join_chunked_probe HashJoinChunkedProbeTest.cpp)

add_turing_test(test_query_ir_cross_product_cut CrossProductCutTest.cpp)
target_include_directories(test_query_ir_cross_product_cut PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_map_property_join MapPropertyJoinTest.cpp)
target_include_directories(test_query_ir_map_property_join PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_cascaded_merge_join CascadedMergeJoinTest.cpp)
target_include_directories(test_query_ir_cascaded_merge_join PRIVATE ${IR_TEST_DIR})

# The chunked hash join cases run hand-written db IR through the lowering at several chunk
# sizes against the shared SimpleGraph.
add_ir_tests(test_query_ir_hash_join_chunked HashJoinChunkedTest.cpp)

# The probe result cases run hand-written nl IR - one program malformed - against the
# shared SimpleGraph.
add_ir_tests(test_query_ir_hash_join_probe_results HashJoinProbeResultsTest.cpp)

# The cost model cases build graphs of their own size and read which form the pipeline
# leaves the cut in, so they need the frontend and the codegen beside the dialects.
add_ir_tests(test_query_ir_hash_join_cost_model HashJoinCostModelTest.cpp)

# The embedding key cases build their own graph of vectors and hold the join's rows against
# the cross product and filter it replaces.
add_ir_tests(test_query_ir_hash_join_embedding_key HashJoinEmbeddingKeyTest.cpp)

# The build index cases group rows by key and walk the buckets directly, with no graph or IR.
add_ir_tests(test_query_ir_hash_join_index NLHashJoinIndexTest.cpp)

# The hash join fusion is checked at both IR levels and run against the shared SimpleGraph,
# so it needs the frontend engine beside the dialects.
add_turing_test(test_query_ir_hash_join HashJoinTest.cpp)

add_turing_test(test_query_ir_nested_product_filter NestedProductFilterTest.cpp)

add_turing_test(test_query_ir_predicate_over_kept_rows PredicateOverKeptRowsTest.cpp)

# The projected-key cases check that a cut still fuses when the query returns the property
# it joins on, so they need the frontend engine beside the dialects.
add_turing_test(test_query_ir_hash_join_projected_key HashJoinProjectedKeyTest.cpp)

# The comma-pattern join key tests read the rows of a four-pattern MATCH as text, so they
# need StringRowSink beside the v3 engine.
add_turing_test(test_query_ir_comma_pattern_join_keys CommaPatternJoinKeysTest.cpp)
target_sources(test_query_ir_comma_pattern_join_keys PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_comma_pattern_join_keys PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_multi_pattern_join MultiPatternJoinTest.cpp)
target_sources(test_query_ir_multi_pattern_join PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_multi_pattern_join PRIVATE ${IR_TEST_DIR})
