add_turing_test(test_query_ir_set_equivalence SetEquivalenceTest.cpp)
target_include_directories(test_query_ir_set_equivalence PRIVATE ${IR_TEST_DIR})

# What a projection behind a SET reads back for the property it wrote.
add_turing_test(test_query_ir_set_readback SetReadbackTest.cpp)

add_turing_test(test_query_ir_mask_property_write MaskPropertyWriteTest.cpp)

add_turing_test(test_query_ir_remove_property RemovePropertyTest.cpp)

add_turing_test(test_query_ir_remove_created_property RemoveCreatedPropertyTest.cpp)

add_turing_test(test_query_ir_uncommitted_remove_read UncommittedRemoveReadTest.cpp)

add_turing_test(test_query_ir_remove_outside_change RemoveOutsideChangeTest.cpp)

add_turing_test(test_query_ir_set_property_null SetPropertyNullTest.cpp)

# The embedding SET-to-null tests need a graph carrying embeddings, which simpledb does
# not, so they build their own.
add_turing_test(test_query_ir_set_embedding_null SetEmbeddingNullTest.cpp)
target_include_directories(test_query_ir_set_embedding_null PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_set_list_property_null SetListPropertyNullTest.cpp)

add_turing_test(test_query_ir_set_property_null_optional_match SetPropertyNullOptionalMatchTest.cpp)

add_turing_test(test_query_ir_set_property_null_multi_part SetPropertyNullMultiPartTest.cpp)

add_turing_test(test_query_ir_set_property_null_expression SetPropertyNullExpressionTest.cpp)

add_turing_test(test_query_ir_set_property_null_pending SetPropertyNullPendingTest.cpp)

add_turing_test(test_query_ir_property_scan_after_set PropertyScanAfterSetTest.cpp)

# A SET writing an element of a list that holds more than one type to a typed property.
add_call_v3_test(test_query_ir_set_list_element_property SetListElementPropertyTest.cpp)

# A write whose value is not of the type the property is registered as.
add_call_v3_test(test_query_ir_set_property_widening SetPropertyWideningTest.cpp)

# SET in the same query part as a CREATE.
add_turing_test(test_query_ir_set_after_create SetAfterCreateTest.cpp)
