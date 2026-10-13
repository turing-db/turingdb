add_turing_test(test_query_ast_structural_expression_comparator StructuralExpressionComparatorTest.cpp)

# What a CALL subquery reports about its body: whether it writes, and whether the clause
# is an updating one.
add_turing_test(test_query_ast_subquery_writes SubqueryWritesToTheGraphTest.cpp)
