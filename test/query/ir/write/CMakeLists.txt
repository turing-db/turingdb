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
target_include_directories(test_query_ir_create_equivalence PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_delete_equivalence DeleteEquivalenceTest.cpp)
target_include_directories(test_query_ir_delete_equivalence PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_optional_match_write OptionalMatchWriteTest.cpp)
target_include_directories(test_query_ir_optional_match_write PRIVATE ${IR_TEST_DIR})

# What a merge keys its entities on: the properties one spec names, told apart from
# another's, and compared against the type the schema holds them as.
add_turing_test(test_query_ir_union_write UnionWriteTest.cpp)

add_turing_test(test_query_ir_create_return CreateReturnTest.cpp)

# The ends each hop of a CREATE chain is written between.
add_turing_test(test_query_ir_create_chain CreateChainTest.cpp)

# DELETE of an entity the same query wrote, which the change holds as a pending write.
add_turing_test(test_query_ir_delete_written DeleteWrittenTest.cpp)

# The pending edges a submit checks its ends for, and the deleted ones it must not.
add_turing_test(test_query_ir_deleted_pending_edge_conflict DeletedPendingEdgeConflictTest.cpp)

add_turing_test(test_query_ir_written_list_property WrittenListPropertyTest.cpp)

add_turing_test(test_query_ir_with_write WithWriteTest.cpp)

add_turing_test(test_query_ir_create_then_delete CreateThenDeleteTest.cpp)
target_include_directories(test_query_ir_create_then_delete PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_list_comprehension_created_entity ListComprehensionCreatedEntityTest.cpp)

add_turing_test(test_query_ir_count_after_write CountAfterWriteTest.cpp)

add_turing_test(test_query_ir_carry_created_entity CarryCreatedEntityTest.cpp)

add_turing_test(test_query_ir_create_set CreateSetTest.cpp)

add_turing_test(test_query_ir_uncommitted_write_count UncommittedWriteCountTest.cpp)

add_turing_test(test_query_ir_match_after_delete MatchAfterDeleteTest.cpp)

add_turing_test(test_query_ir_created_property_lookup CreatedPropertyLookupTest.cpp)

add_turing_test(test_query_ir_path_property_value PathPropertyValueTest.cpp)

add_turing_test(test_query_ir_map_value_write MapValueWriteTest.cpp)

add_turing_test(test_query_ir_set_from_map SetFromMapTest.cpp)

add_turing_test(test_query_ir_set_from_map_creates_property SetFromMapCreatesPropertyTest.cpp)

add_turing_test(test_query_ir_set_merge_from_map SetMergeFromMapTest.cpp)

add_turing_test(test_query_ir_set_from_null SetFromNullTest.cpp)

# A write of a value held in a type-erased cell to a property no write has typed yet.
add_turing_test(test_query_ir_cell_created_property CellCreatedPropertyTest.cpp)

# The rows a SET of type-erased cells does not write, which neither type nor check the property.
add_turing_test(test_query_ir_cell_write_skipped_rows CellWriteSkippedRowsTest.cpp)

# A negative integer written to a property of unsigned integers.
add_turing_test(test_query_ir_negative_integer_unsigned_property NegativeIntegerUnsignedPropertyTest.cpp)

# A CREATE or a MERGE of type-erased cells over a step of no row.
add_turing_test(test_query_ir_cell_write_of_no_row CellWriteOfNoRowTest.cpp)

# A read, in the query that writes it, of a property a write of type-erased cells creates.
add_turing_test(test_query_ir_cell_created_property_read CellCreatedPropertyReadTest.cpp)

# A CREATE or a MERGE of type-erased cells to a property the graph already holds.
add_turing_test(test_query_ir_cell_write_to_existing_property CellWriteToExistingPropertyTest.cpp)

# One write giving a property with no type two types.
add_turing_test(test_query_ir_new_property_two_types NewPropertyTwoTypesTest.cpp)

# The nodes, edges and dataparts db.history reports for a commit COMMIT closes in a change.
add_turing_test(test_query_ir_change_commit_history ChangeCommitHistoryTest.cpp)
