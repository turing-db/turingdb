add_turing_test(test_query_ir_optional_match_cut OptionalMatchCutTest.cpp)
target_include_directories(test_query_ir_optional_match_cut PRIVATE ${IR_TEST_DIR})

# OPTIONAL MATCH beside the clauses around it: a mandatory MATCH, another OPTIONAL MATCH, a
# WITH barrier, an aggregation and a CALL. Reads its rows as text through StringRowSink,
# since the projections range over IDs, properties and lists.
add_turing_test(test_query_ir_optional_match_composition OptionalMatchCompositionTest.cpp)
target_sources(test_query_ir_optional_match_composition PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_optional_match_composition PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_optional_match OptionalMatchTest.cpp)
target_include_directories(test_query_ir_optional_match PRIVATE ${IR_TEST_DIR})
