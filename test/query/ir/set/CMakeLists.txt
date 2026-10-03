add_turing_test(test_query_ir_set_equivalence SetEquivalenceTest.cpp)
target_link_libraries(test_query_ir_set_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_set_equivalence PRIVATE ${IR_TEST_DIR})

# What a projection behind a SET reads back for the property it wrote.
add_turing_test(test_query_ir_set_readback SetReadbackTest.cpp)
target_link_libraries(test_query_ir_set_readback PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_mask_property_write MaskPropertyWriteTest.cpp)
target_link_libraries(test_query_ir_mask_property_write PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_remove_property RemovePropertyTest.cpp)
target_link_libraries(test_query_ir_remove_property PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_remove_created_property RemoveCreatedPropertyTest.cpp)
target_link_libraries(test_query_ir_remove_created_property PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_uncommitted_remove_read UncommittedRemoveReadTest.cpp)
target_link_libraries(test_query_ir_uncommitted_remove_read PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_remove_outside_change RemoveOutsideChangeTest.cpp)
target_link_libraries(test_query_ir_remove_outside_change PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_set_property_null SetPropertyNullTest.cpp)
target_link_libraries(test_query_ir_set_property_null PRIVATE turing_ir_test_write_query_s)

# The embedding SET-to-null tests need a graph carrying embeddings, which simpledb does
# not, so they build their own.
add_turing_test(test_query_ir_set_embedding_null SetEmbeddingNullTest.cpp)
target_link_libraries(test_query_ir_set_embedding_null PRIVATE
        turing_ir_test_rows_s
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_jobs_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_set_embedding_null PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_set_list_property_null SetListPropertyNullTest.cpp)
target_link_libraries(test_query_ir_set_list_property_null PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_set_property_null_optional_match SetPropertyNullOptionalMatchTest.cpp)
target_link_libraries(test_query_ir_set_property_null_optional_match PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_set_property_null_multi_part SetPropertyNullMultiPartTest.cpp)
target_link_libraries(test_query_ir_set_property_null_multi_part PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_set_property_null_expression SetPropertyNullExpressionTest.cpp)
target_link_libraries(test_query_ir_set_property_null_expression PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_set_property_null_pending SetPropertyNullPendingTest.cpp)
target_link_libraries(test_query_ir_set_property_null_pending PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_property_scan_after_set PropertyScanAfterSetTest.cpp)
target_link_libraries(test_query_ir_property_scan_after_set PRIVATE turing_ir_test_write_query_s)

# A SET writing an element of a list that holds more than one type to a typed property.
add_call_v3_test(test_query_ir_set_list_element_property SetListElementPropertyTest.cpp)

# A write whose value is not of the type the property is registered as.
add_call_v3_test(test_query_ir_set_property_widening SetPropertyWideningTest.cpp)

# SET in the same query part as a CREATE.
add_turing_test(test_query_ir_set_after_create SetAfterCreateTest.cpp)
target_link_libraries(test_query_ir_set_after_create PRIVATE turing_ir_test_write_query_s)
