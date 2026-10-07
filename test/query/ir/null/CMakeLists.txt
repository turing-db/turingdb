add_turing_test(test_query_ir_null_predicate NullPredicateTest.cpp)
target_link_libraries(test_query_ir_null_predicate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_null_predicate PRIVATE ${IR_TEST_DIR})

# The null-predicate tests read a Double property that half the fixture's nodes carry, so
# IS NULL and IS NOT NULL over a Double partition it.
add_turing_test(test_query_ir_null_predicate_on_double NullPredicateOnDoubleTest.cpp)
target_sources(test_query_ir_null_predicate_on_double PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_null_predicate_on_double PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_null_predicate_on_double PRIVATE ${IR_TEST_DIR})

# IS NULL against = null: the null test and the comparison are different operators. The bool
# masks and null constants they return need the row sink that renders every column kind.
add_turing_test(test_query_ir_null_comparison NullComparisonTest.cpp)
target_sources(test_query_ir_null_comparison PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_null_comparison PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_null_comparison PRIVATE ${IR_TEST_DIR})

# coalesce, run end to end through QueryInterpreterV3 over the shared SimpleGraph fixture.
add_turing_test(test_query_ir_coalesce_function CoalesceFunctionTest.cpp)
target_link_libraries(test_query_ir_coalesce_function PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_coalesce_function PRIVATE ${IR_TEST_DIR})

# coalesce over an argument list holding a literal, run end to end against simpledb.
add_turing_test(test_query_ir_coalesce_literal_argument CoalesceLiteralArgumentTest.cpp)
target_sources(test_query_ir_coalesce_literal_argument PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_coalesce_literal_argument PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_coalesce_literal_argument PRIVATE ${IR_TEST_DIR})

# A comparison against null disjoined with a constant, run end to end against simpledb.
add_turing_test(test_query_ir_null_comparison_disjunction NullComparisonDisjunctionTest.cpp)
target_sources(test_query_ir_null_comparison_disjunction PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_null_comparison_disjunction PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_null_comparison_disjunction PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_property_of_a_null_variable PropertyOfANullVariableTest.cpp)
target_sources(test_query_ir_property_of_a_null_variable PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_property_of_a_null_variable PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_property_of_a_null_variable PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_null_operand NullOperandTest.cpp)
target_sources(test_query_ir_null_operand PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_null_operand PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_null_operand PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_existence_comparison ExistenceComparisonTest.cpp)
target_link_libraries(test_query_ir_existence_comparison PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_existence_comparison PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_type_is_null TypeIsNullTest.cpp)
target_link_libraries(test_query_ir_type_is_null PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_type_is_null PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_function_null_argument FunctionNullArgumentTest.cpp)
target_link_libraries(test_query_ir_function_null_argument PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_function_null_argument PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_type_test_on_a_null_entity TypeTestOnANullEntityTest.cpp)
target_sources(test_query_ir_type_test_on_a_null_entity PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${IR_TEST_DIR}/CallV3Test.cpp)
target_link_libraries(test_query_ir_type_test_on_a_null_entity PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_db_ir_dbdialect_s
        turing_ir_test_rows_s
        MLIRParser)
target_include_directories(test_query_ir_type_test_on_a_null_entity PRIVATE ${IR_TEST_DIR})
