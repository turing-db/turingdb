add_turing_test(test_query_ir_ldbc_bi11 LdbcBi11Test.cpp)
target_include_directories(test_query_ir_ldbc_bi11 PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_ldbc_is7 LdbcIs7Test.cpp)
target_include_directories(test_query_ir_ldbc_is7 PRIVATE ${IR_TEST_DIR})

# LDBC SNB BI query 16, whose CALL subquery aggregates over an OPTIONAL MATCH.
add_turing_test(test_query_ir_ldbc_bi16 LdbcBi16Test.cpp)
target_include_directories(test_query_ir_ldbc_bi16 PRIVATE ${IR_TEST_DIR})
