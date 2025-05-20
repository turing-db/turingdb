function(add_db_query_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_query_parser_s
                turing_db_query_AST_s
                turing_db_system_s
                turing_db_memory_s
                turing_db_interpreter_s
                turing_db_examples_s
                turing_db_s
                gtest_main
                tabulate)
endfunction()

add_db_query_gtest(test_query_parser QueryParserTest.cpp)
add_db_query_gtest(test_query_multidatapart MultiDatapartQueryTest.cpp)
add_db_query_gtest(test_query QueryTest.cpp)
