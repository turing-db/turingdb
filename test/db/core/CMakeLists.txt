function(add_db_core_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE turing_db_core_s gtest_main bio_import_json_s)
    gtest_discover_tests(${exe_name})
endfunction()

add_db_core_gtest(db_comparator_test ComparatorTest.cpp)

add_db_core_gtest(db_value_types_test ValueTypesTest.cpp)
add_db_core_gtest(db_node_type_test NodeTypeTest.cpp)

add_db_core_gtest(db_network_test NetworkTest.cpp)
add_db_core_gtest(db_node_test NodeTest.cpp)
add_db_core_gtest(db_edge_test EdgeTest.cpp)

# Ranges
add_db_core_gtest(db_network_range_test DBNetworkRangeTest.cpp)
add_db_core_gtest(db_node_type_range_test DBNodeTypeRangeTest.cpp)
add_db_core_gtest(db_edge_type_range_test DBEdgeTypeRangeTest.cpp)
