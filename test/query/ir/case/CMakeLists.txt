# The CASE tests run both forms of the selection end to end through QueryInterpreterV3
# over the shared SimpleGraph fixture.
add_turing_test(test_query_ir_case_expression CaseExpressionTest.cpp)
target_include_directories(test_query_ir_case_expression PRIVATE ${IR_TEST_DIR})

# The db.case dialect tests parse hand-written selections, so they need only the dialect.
add_ir_tests(test_query_ir_case_op CaseOpTest.cpp)

# The Cypher manual's CASE examples, run end to end against simpledb.
add_call_v3_test(test_query_ir_neo4j_case_example Neo4jCaseExampleTest.cpp)

# A CASE over a node or an edge, including the ones an OPTIONAL MATCH left null.
add_call_v3_test(test_query_ir_case_entity_subject CaseEntitySubjectTest.cpp)

add_turing_test(test_query_ir_case_lazy_branch CaseLazyBranchTest.cpp)

add_call_v3_test(test_query_ir_case_list_item_subject CaseListItemSubjectTest.cpp)

add_turing_test(test_query_ir_case_over_projection_alias CaseOverProjectionAliasTest.cpp)
target_sources(test_query_ir_case_over_projection_alias PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_include_directories(test_query_ir_case_over_projection_alias PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_case_entity_inequality CaseEntityInequalityTest.cpp)

add_turing_test(test_query_ir_case_equality_rule CaseEqualityRuleTest.cpp)

add_call_v3_test(test_query_ir_case_branch_value_type CaseBranchValueTypeTest.cpp)

add_call_v3_test(test_query_ir_case_any_binary_operator CaseAnyBinaryOperatorTest.cpp)

add_call_v3_test(test_query_ir_case_any_unary_operator CaseAnyUnaryOperatorTest.cpp)

add_call_v3_test(test_query_ir_case_computed_temporal_branch CaseComputedTemporalBranchTest.cpp)

add_call_v3_test(test_query_ir_case_any_condition CaseAnyConditionTest.cpp)

add_call_v3_test(test_query_ir_case_list_branch_element CaseListBranchElementTest.cpp)

add_call_v3_test(test_query_ir_case_unshaped_list_branch CaseUnshapedListBranchTest.cpp)
