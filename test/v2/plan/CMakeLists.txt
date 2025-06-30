function(add_v2_plan_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_query_parser_s
                turing_db_query_AST_s
                turing_db_system_s
                turing_db_memory_s
                turing_db_v2_plan_s
                turing_db_examples_s
                turing_db_s
                gtest_main
                tabulate)
    gtest_discover_tests(${exe_name}
        DISCOVERY_TIMEOUT 10
        DISCOVER_MODE PRE_TEST)
endfunction()

add_v2_plan_gtest(test_v2_plangen PlanGenTest.cpp)
