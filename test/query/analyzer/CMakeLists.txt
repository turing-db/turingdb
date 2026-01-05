function(test exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_analyzer_s
                turing_db_ast_s
                turing_db_parser_s
                turing_db_examples_s)
endfunction()

test(test_expression ExpressionTest.cpp)
test(test_stmt StmtTest.cpp)
