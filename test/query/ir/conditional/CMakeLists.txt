# CALL subqueries whose body is WHEN ... THEN { ... } ELSE { ... }.
add_turing_test(test_query_ir_conditional_subquery ConditionalSubqueryTest.cpp)

# Conditional branches that write, in a CALL and in a standalone WHEN.
add_turing_test(test_query_ir_conditional_subquery_write ConditionalSubqueryWriteTest.cpp)

# A standalone WHEN ... THEN ... ELSE query.
add_turing_test(test_query_ir_conditional_query ConditionalQueryTest.cpp)

# A WHEN computes a predicate or a branch only for the rows that reach it.
add_turing_test(test_query_ir_conditional_lazy_branch ConditionalLazyBranchTest.cpp)

# A WHEN beside OPTIONAL MATCH, CALL and EXISTS.
add_turing_test(test_query_ir_conditional_composition ConditionalCompositionTest.cpp)

# A WHEN branch returning an entity a MERGE wrote.
add_turing_test(test_query_ir_conditional_merge ConditionalMergeTest.cpp)

# WHEN branches returning an entity one created, another merged and another matched.
add_turing_test(test_query_ir_conditional_create_merge ConditionalCreateMergeTest.cpp)

# `{ WHEN ... } UNION { WHEN ... }`, at the top of a query and in a CALL body.
add_turing_test(test_query_ir_conditional_union ConditionalUnionTest.cpp)

# EXISTS { WHEN ... }: the first branch whose predicate holds answers for the row.
add_turing_test(test_query_ir_conditional_exists_body ConditionalExistsBodyTest.cpp)

# COUNT { WHEN ... }: the rows of the branch the predicates pick, 0 when none runs.
add_turing_test(test_query_ir_conditional_count_body ConditionalCountBodyTest.cpp)

# COLLECT { WHEN ... }: the values of the branch the predicates pick, [] when none runs.
add_turing_test(test_query_ir_conditional_collect_body ConditionalCollectBodyTest.cpp)

# An EXISTS, COUNT or COLLECT over a WHEN in every place an expression stands.
add_turing_test(test_query_ir_conditional_subquery_expression ConditionalSubqueryExpressionTest.cpp)

# A WHEN branch walking to the end set fuse_explore_end_set scans ahead of the function.
add_turing_test(test_query_ir_conditional_explore_end_set ConditionalExploreEndSetTest.cpp)
