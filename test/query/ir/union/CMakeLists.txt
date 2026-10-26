add_turing_test(test_query_ir_union UnionTest.cpp)
target_include_directories(test_query_ir_union PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_union_op UnionOpTest.cpp)

add_turing_test(test_query_ir_union_null_column UnionNullColumnTest.cpp)
target_sources(test_query_ir_union_null_column PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_union_null_column PRIVATE ${IR_TEST_DIR})

# UNION and WHEN branches returning different types: each row keeps the type its branch returned.
add_turing_test(test_query_ir_union_branch_value_type UnionBranchValueTypeTest.cpp)

# A column of lists or maps holds a null wherever a branch can answer one.
add_turing_test(test_query_ir_union_nullable_container_branch UnionNullableContainerBranchTest.cpp)

# A column of paths holds a branch answering null as the empty path.
add_turing_test(test_query_ir_union_path_null_branch UnionPathNullBranchTest.cpp)

# A first branch answering null leaves the column the type the other branches give it.
add_turing_test(test_query_ir_union_null_first_branch UnionNullFirstBranchTest.cpp)
