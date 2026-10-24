# Values of type any - the tagged cells a mixed list, a CASE or a coalesce answers - through
# ORDER BY, DISTINCT, grouping, coalesce and collect, end to end against simpledb.
add_call_v3_test(test_query_ir_any_order_by AnyOrderByTest.cpp)

add_call_v3_test(test_query_ir_any_distinct AnyDistinctTest.cpp)

add_call_v3_test(test_query_ir_any_grouping_key AnyGroupingKeyTest.cpp)

add_call_v3_test(test_query_ir_any_coalesce AnyCoalesceTest.cpp)

add_call_v3_test(test_query_ir_any_collect AnyCollectTest.cpp)

add_call_v3_test(test_query_ir_any_null_test AnyNullTestTest.cpp)

add_call_v3_test(test_query_ir_any_map_value AnyMapValueTest.cpp)

add_call_v3_test(test_query_ir_any_map_value_yielded AnyMapValueYieldedTest.cpp)

add_call_v3_test(test_query_ir_any_map_value_membership AnyMapValueMembershipTest.cpp)

add_call_v3_test(test_query_ir_any_list_concatenation_null AnyListConcatenationNullTest.cpp)

add_call_v3_test(test_query_ir_any_cell_text_concatenation AnyCellTextConcatenationTest.cpp)

add_call_v3_test(test_query_ir_any_reduce_null_body AnyReduceNullBodyTest.cpp)

add_call_v3_test(test_query_ir_any_map_value_collected AnyMapValueCollectedTest.cpp)

add_call_v3_test(test_query_ir_any_cell_plus_null AnyCellPlusNullTest.cpp)
