add_turing_test(test_query_ir_ldbc_bi11 LdbcBi11Test.cpp)
target_link_libraries(test_query_ir_ldbc_bi11 PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_ldbc_bi11 PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_ldbc_is7 LdbcIs7Test.cpp)
target_link_libraries(test_query_ir_ldbc_is7 PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_ldbc_is7 PRIVATE ${IR_TEST_DIR})

# LDBC SNB BI query 16, whose CALL subquery aggregates over an OPTIONAL MATCH.
add_turing_test(test_query_ir_ldbc_bi16 LdbcBi16Test.cpp)
target_link_libraries(test_query_ir_ldbc_bi16 PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_ldbc_bi16 PRIVATE ${IR_TEST_DIR})
