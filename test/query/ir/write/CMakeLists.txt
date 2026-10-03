add_ir_tests(test_query_ir_create_node_edge CreateNodeEdgeTest.cpp)

add_call_v3_test(test_query_ir_create_with CreateWithTest.cpp)

add_call_v3_test(test_query_ir_create_edge_property CreateEdgePropertyTest.cpp)

add_call_v3_test(test_query_ir_match_created_edge MatchCreatedEdgeTest.cpp)

add_call_v3_test(test_query_ir_match_created_edge_by_label MatchCreatedEdgeByLabelTest.cpp)

add_call_v3_test(test_query_ir_match_created_edge_by_type_and_label MatchCreatedEdgeByTypeAndLabelTest.cpp)

add_call_v3_test(test_query_ir_scan_created_entity ScanCreatedEntityTest.cpp)

add_call_v3_test(test_query_ir_explore_created_edge ExploreCreatedEdgeTest.cpp)

add_call_v3_test(test_query_ir_write_pattern_inline_where WritePatternInlineWhereTest.cpp)

add_turing_test(test_query_ir_create_equivalence CreateEquivalenceTest.cpp)
target_link_libraries(test_query_ir_create_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_create_equivalence PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_delete_equivalence DeleteEquivalenceTest.cpp)
target_link_libraries(test_query_ir_delete_equivalence PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_delete_equivalence PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_optional_match_write OptionalMatchWriteTest.cpp)
target_link_libraries(test_query_ir_optional_match_write PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_optional_match_write PRIVATE ${IR_TEST_DIR})

# What a merge keys its entities on: the properties one spec names, told apart from
# another's, and compared against the type the schema holds them as.
add_turing_test(test_query_ir_union_write UnionWriteTest.cpp)
target_link_libraries(test_query_ir_union_write PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_create_return CreateReturnTest.cpp)
target_link_libraries(test_query_ir_create_return PRIVATE turing_ir_test_write_query_s)

# The ends each hop of a CREATE chain is written between.
add_turing_test(test_query_ir_create_chain CreateChainTest.cpp)
target_link_libraries(test_query_ir_create_chain PRIVATE turing_ir_test_write_query_s)

# DELETE of an entity the same query wrote, which the change holds as a pending write.
add_turing_test(test_query_ir_delete_written DeleteWrittenTest.cpp)
target_link_libraries(test_query_ir_delete_written PRIVATE turing_ir_test_write_query_s)

# The pending edges a submit checks its ends for, and the deleted ones it must not.
add_turing_test(test_query_ir_deleted_pending_edge_conflict DeletedPendingEdgeConflictTest.cpp)
target_link_libraries(test_query_ir_deleted_pending_edge_conflict PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_written_list_property WrittenListPropertyTest.cpp)
target_link_libraries(test_query_ir_written_list_property PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_with_write WithWriteTest.cpp)
target_link_libraries(test_query_ir_with_write PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_create_then_delete CreateThenDeleteTest.cpp)
target_link_libraries(test_query_ir_create_then_delete PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_create_then_delete PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_comprehension_created_entity ListComprehensionCreatedEntityTest.cpp)
target_link_libraries(test_query_ir_list_comprehension_created_entity PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_count_after_write CountAfterWriteTest.cpp)
target_link_libraries(test_query_ir_count_after_write PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_carry_created_entity CarryCreatedEntityTest.cpp)
target_link_libraries(test_query_ir_carry_created_entity PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_create_set CreateSetTest.cpp)
target_link_libraries(test_query_ir_create_set PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_uncommitted_write_count UncommittedWriteCountTest.cpp)
target_link_libraries(test_query_ir_uncommitted_write_count PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_match_after_delete MatchAfterDeleteTest.cpp)
target_link_libraries(test_query_ir_match_after_delete PRIVATE turing_ir_test_write_query_s)
