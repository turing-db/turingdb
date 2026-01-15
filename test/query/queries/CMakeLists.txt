function(add_queries_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_examples_s
                turing_db_s
                turing_testenv_s)
    target_include_directories(${exe_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

add_queries_gtest(test_queries QueriesTest.cpp)
add_queries_gtest(test_filter_predicates FilterPredicatesTest.cpp)
add_queries_gtest(test_delete_queries DeleteQueriesTest.cpp)
add_queries_gtest(test_write_queries WriteQueriesTest.cpp)
add_queries_gtest(test_load_graph LoadGraphTest.cpp)
add_queries_gtest(test_create_graph CreateGraphTest.cpp)
add_queries_gtest(test_change_queries ChangeQueriesTest.cpp)
add_queries_gtest(test_complex_null_predicates ComplexNullPredicatesTest.cpp)
add_queries_gtest(test_multi_hop_pattern MultiHopPatternTest.cpp)
add_queries_gtest(test_auto_create_command AutoCreateCommandTest.cpp)
add_queries_gtest(test_match_create MatchCreateTest.cpp)
add_queries_gtest(test_string_filter StringFilterTest.cpp)
add_queries_gtest(test_join_feature AutoJoinTest.cpp)
