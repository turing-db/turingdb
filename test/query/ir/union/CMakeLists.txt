add_turing_test(test_query_ir_union UnionTest.cpp)
target_link_libraries(test_query_ir_union PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_union PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_union_op UnionOpTest.cpp)

add_turing_test(test_query_ir_union_null_column UnionNullColumnTest.cpp)
target_sources(test_query_ir_union_null_column PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_union_null_column PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_union_null_column PRIVATE ${IR_TEST_DIR})
