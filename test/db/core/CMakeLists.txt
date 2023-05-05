
add_executable(db_string_tests StringRefTest.cpp)
target_link_libraries(db_string_tests PRIVATE
        turing_db_core_s
        gtest_main)

add_executable(db_value_types_test ValueTypesTest.cpp)
target_link_libraries(db_value_types_test PRIVATE
        turing_db_core_s
        gtest_main)

add_executable(db_node_type_test NodeTypeTest.cpp)
target_link_libraries(db_node_type_test PRIVATE
        turing_db_core_s
        gtest_main)

add_executable(db_network_test NetworkTest.cpp)
target_link_libraries(db_network_test PRIVATE turing_db_core_s gtest_main)

add_executable(db_node_test NodeTest.cpp)
target_link_libraries(db_node_test PRIVATE turing_db_core_s gtest_main)

add_executable(db_edge_test EdgeTest.cpp)
target_link_libraries(db_edge_test PRIVATE turing_db_core_s gtest_main)

add_executable(db_component_type_test ComponentTypeTest.cpp)
target_link_libraries(db_component_type_test PRIVATE turing_db_core_s gtest_main)

gtest_discover_tests(db_string_tests)
gtest_discover_tests(db_value_types_test)
gtest_discover_tests(db_network_test)
gtest_discover_tests(db_node_type_test)
gtest_discover_tests(db_node_test)
gtest_discover_tests(db_component_type_test)
