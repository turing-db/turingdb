# The distinct-key rule nl.make_map shares with db.make_map.
add_ir_tests(test_query_ir_nl_make_map_keys NLMakeMapKeysTest.cpp)
target_link_libraries(test_query_ir_nl_make_map_keys PRIVATE turing_db_ir_nldialect_s)

add_turing_test(test_query_ir_cypher_map_literal_rows CypherMapLiteralRowsTest.cpp)
target_link_libraries(test_query_ir_cypher_map_literal_rows PRIVATE
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
target_include_directories(test_query_ir_cypher_map_literal_rows PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_of_maps ListOfMapsTest.cpp)
target_link_libraries(test_query_ir_list_of_maps PRIVATE
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
target_include_directories(test_query_ir_list_of_maps PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_cypher_dynamic_map_literal CypherDynamicMapLiteralTest.cpp)
target_link_libraries(test_query_ir_cypher_dynamic_map_literal PRIVATE
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
target_include_directories(test_query_ir_cypher_dynamic_map_literal PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_map_literal_property_value MapLiteralPropertyValueTest.cpp)
target_link_libraries(test_query_ir_map_literal_property_value PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_ast_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_frontend_cypher_s
        turing_db_storage_s)
target_include_directories(test_query_ir_map_literal_property_value PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_map_property MapPropertyTest.cpp)
target_link_libraries(test_query_ir_map_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_map_property PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_of_map_properties ListOfMapPropertiesTest.cpp)
target_link_libraries(test_query_ir_list_of_map_properties PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_list_of_map_properties PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_map_against_other_type MapAgainstOtherTypeTest.cpp)
target_link_libraries(test_query_ir_map_against_other_type PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_map_against_other_type PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_map_property_equality MapPropertyEqualityTest.cpp)
target_link_libraries(test_query_ir_map_property_equality PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_map_property_equality PRIVATE ${IR_TEST_DIR})
