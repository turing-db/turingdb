add_call_v3_test(test_query_ir_per_row_value_memory PerRowValueMemoryTest.cpp)

add_turing_test(test_query_ir_retained_values_across_chunks RetainedValuesAcrossChunksTest.cpp)
target_link_libraries(test_query_ir_retained_values_across_chunks PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_retained_values_across_chunks PRIVATE ${IR_TEST_DIR})
