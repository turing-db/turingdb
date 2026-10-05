# CALL subqueries whose body is WHEN ... THEN { ... } ELSE { ... }.
add_turing_test(test_query_ir_conditional_subquery ConditionalSubqueryTest.cpp)
target_link_libraries(test_query_ir_conditional_subquery PRIVATE turing_ir_test_write_query_s)

# Conditional branches that write, in a CALL and in a standalone WHEN.
add_turing_test(test_query_ir_conditional_subquery_write ConditionalSubqueryWriteTest.cpp)
target_link_libraries(test_query_ir_conditional_subquery_write PRIVATE turing_ir_test_write_query_s)

# A standalone WHEN ... THEN ... ELSE query.
add_turing_test(test_query_ir_conditional_query ConditionalQueryTest.cpp)
target_link_libraries(test_query_ir_conditional_query PRIVATE turing_ir_test_write_query_s)

# A WHEN computes a predicate or a branch only for the rows that reach it.
add_turing_test(test_query_ir_conditional_lazy_branch ConditionalLazyBranchTest.cpp)
target_link_libraries(test_query_ir_conditional_lazy_branch PRIVATE turing_ir_test_write_query_s)

# A WHEN beside OPTIONAL MATCH, CALL and EXISTS.
add_turing_test(test_query_ir_conditional_composition ConditionalCompositionTest.cpp)
target_link_libraries(test_query_ir_conditional_composition PRIVATE turing_ir_test_write_query_s)

# A WHEN branch returning an entity a MERGE wrote.
add_turing_test(test_query_ir_conditional_merge ConditionalMergeTest.cpp)
target_link_libraries(test_query_ir_conditional_merge PRIVATE turing_ir_test_write_query_s)

# WHEN branches returning an entity one created, another merged and another matched.
add_turing_test(test_query_ir_conditional_create_merge ConditionalCreateMergeTest.cpp)
target_link_libraries(test_query_ir_conditional_create_merge PRIVATE turing_ir_test_write_query_s)

# `{ WHEN ... } UNION { WHEN ... }`, at the top of a query and in a CALL body.
add_turing_test(test_query_ir_conditional_union ConditionalUnionTest.cpp)
target_link_libraries(test_query_ir_conditional_union PRIVATE turing_ir_test_write_query_s)
