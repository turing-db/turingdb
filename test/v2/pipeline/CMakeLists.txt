function(add_v2_pipeline_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_v2_pipeline_s
                turing_db_system_s
                turing_db_memory_s
                turing_db_examples_s
                turing_db_s
                turing_testenv_s
                tabulate)
endfunction()

add_v2_pipeline_gtest(test_v2_pipeline PipelineTest.cpp)
