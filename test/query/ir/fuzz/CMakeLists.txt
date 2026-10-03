add_turing_test(test_query_ir_fuzz_misshapen_column_vectors FuzzMisshapenColumnVectorsTest.cpp)
target_link_libraries(test_query_ir_fuzz_misshapen_column_vectors PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_misshapen_column_masks FuzzMisshapenColumnMasksTest.cpp)
target_link_libraries(test_query_ir_fuzz_misshapen_column_masks PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_property_null_binary_operation FuzzPropertyNullBinaryOperationTest.cpp)
target_link_libraries(test_query_ir_fuzz_property_null_binary_operation PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_component_var_not_registered FuzzComponentVarNotRegisteredTest.cpp)
target_link_libraries(test_query_ir_fuzz_component_var_not_registered PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_hang FuzzHangTest.cpp)
target_link_libraries(test_query_ir_fuzz_hang PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
set_tests_properties(test_query_ir_fuzz_hang PROPERTIES TIMEOUT 120)

add_turing_test(test_query_ir_fuzz_aggregate_without_arguments FuzzAggregateWithoutArgumentsTest.cpp)
target_link_libraries(test_query_ir_fuzz_aggregate_without_arguments PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_misshapen_binary_operands FuzzMisshapenBinaryOperandsTest.cpp)
target_link_libraries(test_query_ir_fuzz_misshapen_binary_operands PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_node_id_binary_operation FuzzNodeIDBinaryOperationTest.cpp)
target_link_libraries(test_query_ir_fuzz_node_id_binary_operation PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_function_of_nullable_constant FuzzFunctionOfNullableConstantTest.cpp)
target_link_libraries(test_query_ir_fuzz_function_of_nullable_constant PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_pattern_comprehension_in_list_comprehension FuzzPatternComprehensionInListComprehensionTest.cpp)
target_link_libraries(test_query_ir_fuzz_pattern_comprehension_in_list_comprehension PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_edge_variable_from_unwound_node FuzzEdgeVariableFromUnwoundNodeTest.cpp)
target_link_libraries(test_query_ir_fuzz_edge_variable_from_unwound_node PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_mask_binary_operation FuzzMaskBinaryOperationTest.cpp)
target_link_libraries(test_query_ir_fuzz_mask_binary_operation PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_case_comparison FuzzCaseComparisonTest.cpp)
target_link_libraries(test_query_ir_fuzz_case_comparison PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_embedding_in_list FuzzEmbeddingInListTest.cpp)
target_link_libraries(test_query_ir_fuzz_embedding_in_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_entity_in_list FuzzEntityInListTest.cpp)
target_link_libraries(test_query_ir_fuzz_entity_in_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_fuzz_computed_null_in_nested_list FuzzComputedNullInNestedListTest.cpp)
target_link_libraries(test_query_ir_fuzz_computed_null_in_nested_list PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
