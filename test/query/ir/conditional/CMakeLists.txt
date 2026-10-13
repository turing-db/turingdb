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
