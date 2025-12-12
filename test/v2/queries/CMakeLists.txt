function(add_v2_queries_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_examples_s
                turing_db_s
                turing_testenv_s)
    target_include_directories(${exe_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

add_v2_queries_gtest(test_v2_queries QueriesTest.cpp)
add_v2_queries_gtest(test_v2_delete_queries DeleteQueriesTest.cpp)
add_v2_queries_gtest(test_v2_write_queries WriteQueriesTest.cpp)
add_v2_queries_gtest(test_v2_load_graph LoadGraphTest.cpp)
