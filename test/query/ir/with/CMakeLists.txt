add_turing_test(test_query_ir_with WithTest.cpp)
target_include_directories(test_query_ir_with PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_with_multi_part WithMultiPartTest.cpp)
target_include_directories(test_query_ir_with_multi_part PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_with_cut WithCutTest.cpp)
target_include_directories(test_query_ir_with_cut PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_with_scope WithScopeTest.cpp)
target_include_directories(test_query_ir_with_scope PRIVATE ${IR_TEST_DIR})

# An aggregate aliased to the name it reads, run end to end against simpledb.
add_turing_test(test_query_ir_shadowed_alias ShadowedAliasTest.cpp)
target_include_directories(test_query_ir_shadowed_alias PRIVATE ${IR_TEST_DIR})

# The scope the items of a projection read, and the one their aliases bind in, against simpledb.
add_turing_test(test_query_ir_projection_alias_scope ProjectionAliasScopeTest.cpp)
target_include_directories(test_query_ir_projection_alias_scope PRIVATE ${IR_TEST_DIR})

# A five-part query picking entities out of the lists its barriers collect, run end to end
# against simpledb.
add_turing_test(test_query_ir_multi_part_entity_pick MultiPartEntityPickTest.cpp)
target_sources(test_query_ir_multi_part_entity_pick PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_multi_part_entity_pick PRIVATE ${IR_TEST_DIR})

# The WHERE of a WITH reading a variable the projection drops.
add_turing_test(test_query_ir_with_where_dropped_variable WithWhereDroppedVariableTest.cpp)

# The ORDER BY of a WITH reading a variable the projection drops.
add_turing_test(test_query_ir_with_order_by_dropped_variable WithOrderByDroppedVariableTest.cpp)

add_turing_test(test_query_ir_constant_projection_over_reduced_rows ConstantProjectionOverReducedRowsTest.cpp)
target_sources(test_query_ir_constant_projection_over_reduced_rows PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_constant_projection_over_reduced_rows PRIVATE ${IR_TEST_DIR})
