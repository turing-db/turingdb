
add_executable(db_string_tests StringRefTest.cpp)
target_link_libraries(db_string_tests PRIVATE
        turing_db_s
        gtest_main)

add_executable(db_value_types_test ValueTypesTest.cpp)
target_link_libraries(db_value_types_test PRIVATE
        turing_db_s
        gtest_main)

#add_executable(db_object_types_test ObjectTypeTest.cpp)
#target_link_libraries(db_object_types_test PRIVATE turing_db_s gtest_main)

#add_executable(db_network_test NetworkTest.cpp)
#target_link_libraries(db_network_test PRIVATE turing_db_s gtest_main)

gtest_discover_tests(db_string_tests)
gtest_discover_tests(db_value_types_test)
#gtest_discover_tests(db_object_types_test)
#gtest_discover_tests(db_network_test)
