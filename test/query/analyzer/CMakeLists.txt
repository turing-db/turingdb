function(test exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_analyzer_s
                turing_db_ast_s
                turing_db_parser_s
                turing_db_examples_s)
endfunction()

test(test_expression ExpressionTest.cpp)
test(test_stmt StmtTest.cpp)
test(test_distinct_order_by DistinctOrderByTest.cpp)
test(test_order_by_aggregate_alias OrderByAggregateAliasTest.cpp)
test(test_projection_variable_alias ProjectionVariableAliasTest.cpp)
test(test_nested_aggregate NestedAggregateTest.cpp)
test(test_procedure_constant_argument ProcedureConstantArgumentTest.cpp)
test(test_procedure_yield_rebinding ProcedureYieldRebindingTest.cpp)
test(test_procedure_call_rejection ProcedureCallRejectionTest.cpp)
test(test_vector_search_query_vector VectorSearchQueryVectorTest.cpp)
