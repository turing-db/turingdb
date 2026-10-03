# The CASE tests run both forms of the selection end to end through QueryInterpreterV3
# over the shared SimpleGraph fixture.
add_turing_test(test_query_ir_case_expression CaseExpressionTest.cpp)
target_link_libraries(test_query_ir_case_expression PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_case_expression PRIVATE ${IR_TEST_DIR})

# The db.case dialect tests parse hand-written selections, so they need only the dialect.
add_ir_tests(test_query_ir_case_op CaseOpTest.cpp)

# The Cypher manual's CASE examples, run end to end against simpledb.
add_call_v3_test(test_query_ir_neo4j_case_example Neo4jCaseExampleTest.cpp)

# A CASE over a node or an edge, including the ones an OPTIONAL MATCH left null.
add_call_v3_test(test_query_ir_case_entity_subject CaseEntitySubjectTest.cpp)

add_turing_test(test_query_ir_case_lazy_branch CaseLazyBranchTest.cpp)
target_link_libraries(test_query_ir_case_lazy_branch PRIVATE turing_ir_test_write_query_s)
