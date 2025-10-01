function(add_v2_plan_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_v2_parser_s
                turing_db_v2_analyzer_s
                turing_db_v2_ast_s
                turing_db_system_s
                turing_db_memory_s
                turing_db_v2_plan_s
                turing_db_s
                turing_testenv_s
                tabulate)
endfunction()

add_v2_plan_gtest(test_v2_plangen PlanGenTest.cpp)
