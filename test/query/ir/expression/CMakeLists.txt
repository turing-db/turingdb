add_turing_test(test_query_ir_function_input_column FunctionInputColumnTest.cpp)
target_link_libraries(test_query_ir_function_input_column PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_function_input_column PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_arithmetic_ext ArithmeticExtTest.cpp)
target_link_libraries(test_query_ir_arithmetic_ext PRIVATE
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
target_include_directories(test_query_ir_arithmetic_ext PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_string_concat StringConcatTest.cpp)
target_link_libraries(test_query_ir_string_concat PRIVATE
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
target_include_directories(test_query_ir_string_concat PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_absent_property_name AbsentPropertyNameTest.cpp)
target_link_libraries(test_query_ir_absent_property_name PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_absent_property_name PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_absent_schema_name AbsentSchemaNameTest.cpp)
target_link_libraries(test_query_ir_absent_schema_name PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_absent_schema_name PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_string_size StringSizeTest.cpp)
target_link_libraries(test_query_ir_string_size PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_string_size PRIVATE ${IR_TEST_DIR})

# The crossed-source constant tests run a query whose constant is in flight when a search
# or a CALL opens a product, and read their rows as text.
add_turing_test(test_query_ir_crossed_source_constant CrossedSourceConstantTest.cpp)
target_link_libraries(test_query_ir_crossed_source_constant PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_crossed_source_constant PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_string_predicates StringPredicateTest.cpp)
target_sources(test_query_ir_string_predicates PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_string_predicates PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_string_predicates PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_numeric_conversion NumericConversionTest.cpp)
target_sources(test_query_ir_numeric_conversion PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_numeric_conversion PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_numeric_conversion PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_mask_comparison MaskComparisonTest.cpp)
target_link_libraries(test_query_ir_mask_comparison PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_mask_comparison PRIVATE ${IR_TEST_DIR})

# type() over an edge, run end to end against simpledb.
add_call_v3_test(test_query_ir_edge_type_function EdgeTypeFunctionTest.cpp)

# id() over the nodes and edges of simpledb, run end to end.
add_turing_test(test_query_ir_id_function IdFunctionTest.cpp)
target_link_libraries(test_query_ir_id_function PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_id_function PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_cross_type_comparison CrossTypeComparisonTest.cpp)
target_link_libraries(test_query_ir_cross_type_comparison PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_cross_type_comparison PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_to_string_function ToStringFunctionTest.cpp)
target_link_libraries(test_query_ir_to_string_function PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_to_string_function PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_number_concatenation NumberConcatenationTest.cpp)
target_link_libraries(test_query_ir_number_concatenation PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_number_concatenation PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_tagged_cell_operator TaggedCellOperatorTest.cpp)
target_link_libraries(test_query_ir_tagged_cell_operator PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_tagged_cell_operator PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_tagged_cell_function TaggedCellFunctionTest.cpp)
target_link_libraries(test_query_ir_tagged_cell_function PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_tagged_cell_function PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_concat_operator ConcatOperatorTest.cpp)
target_link_libraries(test_query_ir_concat_operator PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_concat_operator PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_tagged_cell_comparison TaggedCellComparisonTest.cpp)
target_sources(test_query_ir_tagged_cell_comparison PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_tagged_cell_comparison PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_tagged_cell_comparison PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_parenthesised_property ParenthesisedPropertyTest.cpp)
target_link_libraries(test_query_ir_parenthesised_property PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_function_call_property FunctionCallPropertyTest.cpp)
target_link_libraries(test_query_ir_function_call_property PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_numeric_function NumericFunctionTest.cpp)
target_link_libraries(test_query_ir_numeric_function PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_string_function StringFunctionTest.cpp)
target_link_libraries(test_query_ir_string_function PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_string_argument_function StringArgumentFunctionTest.cpp)
target_link_libraries(test_query_ir_string_argument_function PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_untyped_element_function UntypedElementFunctionTest.cpp)
target_link_libraries(test_query_ir_untyped_element_function PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_function_argument_source FunctionArgumentSourceTest.cpp)
target_link_libraries(test_query_ir_function_argument_source PRIVATE turing_ir_test_write_query_s)

# A comparison chain's middle operand is one expression both of its sides point at, read
# off the codegen dump.
add_ir_tests(test_query_ir_comparison_chain_codegen ComparisonChainCodegenTest.cpp)
target_sources(test_query_ir_comparison_chain_codegen PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_comparison_chain_codegen PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_comparison_chain_codegen PRIVATE ${IR_TEST_DIR})

add_ir_tests(test_query_ir_foreign_filter_column ForeignFilterColumnTest.cpp)

add_turing_test(test_query_ir_double_cell_equality DoubleCellEqualityTest.cpp)
target_link_libraries(test_query_ir_double_cell_equality PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_const_conversion_property ConstConversionPropertyTest.cpp)
target_link_libraries(test_query_ir_const_conversion_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_const_conversion_property PRIVATE ${IR_TEST_DIR})

# Equality over the double a type-erased list cell computes its arithmetic in, which the
# analyzer types as the integer the list's null element leaves it.
add_turing_test(test_query_ir_tagged_cell_arithmetic_equality TaggedCellArithmeticEqualityTest.cpp)
target_link_libraries(test_query_ir_tagged_cell_arithmetic_equality PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_call_v3_test(test_query_ir_tagged_cell_string_addition TaggedCellStringAdditionTest.cpp)

add_turing_test(test_query_ir_integer_division_overflow IntegerDivisionOverflowTest.cpp)
target_link_libraries(test_query_ir_integer_division_overflow PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_keys_and_properties KeysAndPropertiesTest.cpp)
target_link_libraries(test_query_ir_keys_and_properties PRIVATE turing_ir_test_write_query_s)
