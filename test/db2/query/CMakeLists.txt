function(add_db2_query_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_query_parser_s
                turing_db2_query_AST_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_query_gtest(db2_query_parser_test QueryParserTest.cpp)
