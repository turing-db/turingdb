function(test exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_v2_analyzer_s)
endfunction()

test(test_expression ExpressionTest.cpp)
