add_turing_test(test_query_ast_structural_expression_comparator StructuralExpressionComparatorTest.cpp)
target_link_libraries(test_query_ast_structural_expression_comparator
    PRIVATE turing_db_ast_s)

# What a CALL subquery reports about its body: whether it writes, and whether the clause
# is an updating one.
add_turing_test(test_query_ast_subquery_writes SubqueryWritesToTheGraphTest.cpp)
target_link_libraries(test_query_ast_subquery_writes
    PRIVATE turing_db_ast_s)
