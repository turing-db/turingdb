add_turing_test(test_query_ir_union UnionTest.cpp)
target_include_directories(test_query_ir_union PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_union_op UnionOpTest.cpp)

add_turing_test(test_query_ir_union_null_column UnionNullColumnTest.cpp)
target_sources(test_query_ir_union_null_column PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_union_null_column PRIVATE ${IR_TEST_DIR})
