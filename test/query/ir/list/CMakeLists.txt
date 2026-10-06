add_call_v3_test(test_query_ir_concatenated_list_seed ConcatenatedListSeedTest.cpp)

add_turing_test(test_query_ir_list_concat ListConcatTest.cpp)
target_link_libraries(test_query_ir_list_concat PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_list_concat PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_index ListIndexTest.cpp)
target_link_libraries(test_query_ir_list_index PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_list_index PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_cypher_list_literal CypherListLiteralTest.cpp)
target_link_libraries(test_query_ir_cypher_list_literal PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_exec_s
        turing_db_ir_lowering_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_cypher_list_literal PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_literal_element_type ListLiteralElementTypeTest.cpp)
target_link_libraries(test_query_ir_list_literal_element_type PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_ir_codegen_s
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s
        turing_db_ir_storagedialect_s
        turing_db_ir_common_s
        turing_db_storage_s
        MLIRParser)
target_include_directories(test_query_ir_list_literal_element_type PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_literal_property_value ListLiteralPropertyValueTest.cpp)
target_link_libraries(test_query_ir_list_literal_property_value PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_storage_s)
target_include_directories(test_query_ir_list_literal_property_value PRIVATE ${IR_TEST_DIR})

# An element indexed out of a list bound by an UNWIND over a list of lists.
add_turing_test(test_query_ir_nested_list_index_property NestedListIndexPropertyTest.cpp)
target_link_libraries(test_query_ir_nested_list_index_property PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_list_property ListPropertyTest.cpp)
target_link_libraries(test_query_ir_list_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_property PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_property_equality ListPropertyEqualityTest.cpp)
target_link_libraries(test_query_ir_list_property_equality PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_property_equality PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_functions ListFunctionsTest.cpp)
target_link_libraries(test_query_ir_list_functions PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_functions PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_range_function RangeFunctionTest.cpp)
target_link_libraries(test_query_ir_range_function PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_range_function PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_property_order_collect ListPropertyOrderCollectTest.cpp)
target_link_libraries(test_query_ir_list_property_order_collect PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_property_order_collect PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_property_in ListPropertyInTest.cpp)
target_link_libraries(test_query_ir_list_property_in PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_list_property_index_concat ListPropertyIndexConcatTest.cpp)
target_link_libraries(test_query_ir_list_property_index_concat PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_list_property_comprehension ListPropertyComprehensionTest.cpp)
target_link_libraries(test_query_ir_list_property_comprehension PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_list_equality ListEqualityTest.cpp)
target_sources(test_query_ir_list_equality PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_list_equality PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_list_equality PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_in ListInTest.cpp)
target_sources(test_query_ir_list_in PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_list_in PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_list_in PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_return_heterogeneous_list ReturnHeterogeneousListTest.cpp)
target_link_libraries(test_query_ir_return_heterogeneous_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_return_heterogeneous_list PRIVATE ${IR_TEST_DIR})

# An indexed list element as the operand of an operator, run end to end against simpledb.
add_turing_test(test_query_ir_list_element_operand ListElementOperandTest.cpp)
target_sources(test_query_ir_list_element_operand PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_list_element_operand PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_list_element_operand PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_scalar_concat ListScalarConcatTest.cpp)
target_link_libraries(test_query_ir_list_scalar_concat PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_scalar_concat PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_slice ListSliceTest.cpp)
target_link_libraries(test_query_ir_list_slice PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_slice PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_keyword ListKeywordTest.cpp)
target_link_libraries(test_query_ir_list_keyword PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_keyword PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_predicate ListPredicateTest.cpp)
target_link_libraries(test_query_ir_list_predicate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_predicate PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_element_ops_over_a_constant_list ElementOpsOverAConstantListTest.cpp)
target_sources(test_query_ir_element_ops_over_a_constant_list PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_element_ops_over_a_constant_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_element_ops_over_a_constant_list PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_predicate_over_booleans ListPredicateOverBooleansTest.cpp)
target_sources(test_query_ir_list_predicate_over_booleans PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_list_predicate_over_booleans PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_predicate_over_booleans PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_predicate_over_a_list_property ListPredicateOverAListPropertyTest.cpp)
target_sources(test_query_ir_list_predicate_over_a_list_property PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_list_predicate_over_a_list_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_predicate_over_a_list_property PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_predicate_over_a_null_list ListPredicateOverANullListTest.cpp)
target_sources(test_query_ir_list_predicate_over_a_null_list PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_list_predicate_over_a_null_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_predicate_over_a_null_list PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_head_and_last HeadAndLastTest.cpp)
target_sources(test_query_ir_head_and_last PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_head_and_last PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_head_and_last PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_predicate_body_shapes ListPredicateBodyShapesTest.cpp)
target_sources(test_query_ir_list_predicate_body_shapes PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_list_predicate_body_shapes PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_predicate_body_shapes PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_constant_list_index ConstantListIndexTest.cpp)
target_sources(test_query_ir_constant_list_index PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_constant_list_index PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_constant_list_index PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_comprehension ListComprehensionTest.cpp)
target_link_libraries(test_query_ir_list_comprehension PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_comprehension PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_comprehension_edge_variable ListComprehensionEdgeVariableTest.cpp)
target_link_libraries(test_query_ir_list_comprehension_edge_variable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_comprehension_edge_variable PRIVATE ${IR_TEST_DIR})

# The misaligned program is hand-written nl, so it runs against the SimpleGraph fixture
# through the interpreter rather than through a query.
add_ir_tests(test_query_ir_list_comprehension_row_alignment ListComprehensionRowAlignmentTest.cpp)
target_link_libraries(test_query_ir_list_comprehension_row_alignment PRIVATE
        turing_db_examples_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_list_comprehension_outer_variable ListComprehensionOuterVariableTest.cpp)
target_sources(test_query_ir_list_comprehension_outer_variable PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_list_comprehension_outer_variable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_list_comprehension_outer_variable PRIVATE ${IR_TEST_DIR})

# Arithmetic inside a list literal.
add_call_v3_test(test_query_ir_list_literal_arithmetic ListLiteralArithmeticTest.cpp)

# range() over the full Int64 span, whose integer count does not fit 64 bits.
add_call_v3_test(test_query_ir_range_span_overflow RangeSpanOverflowTest.cpp)

# Arithmetic on an element of a list that holds a property the graph does not have.
add_call_v3_test(test_query_ir_missing_property_list_element MissingPropertyListElementTest.cpp)

add_turing_test(test_query_ir_null_in_list_element NullInListElementTest.cpp)
target_sources(test_query_ir_null_in_list_element PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_null_in_list_element PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_null_in_list_element PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_in_list ListInListTest.cpp)
target_sources(test_query_ir_list_in_list PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_list_in_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_list_in_list PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_ordering ListOrderingTest.cpp)
target_sources(test_query_ir_list_ordering PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_list_ordering PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_ordering PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_node_in_integer_list NodeInIntegerListTest.cpp)
target_link_libraries(test_query_ir_node_in_integer_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_list_reverse ListReverseTest.cpp)
target_link_libraries(test_query_ir_list_reverse PRIVATE turing_ir_test_write_query_s)
