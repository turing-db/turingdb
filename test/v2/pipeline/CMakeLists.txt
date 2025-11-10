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
    target_include_directories(${exe_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endfunction()

add_v2_pipeline_gtest(test_v2_pipeline PipelineTest.cpp)
add_v2_pipeline_gtest(test_v2_pipeline_ScanNodesProcessor processors/ScanNodesProcessorTest.cpp)
add_v2_pipeline_gtest(test_v2_pipeline_GetOutEdgesProcessor processors/GetOutEdgesProcessorTest.cpp)
add_v2_pipeline_gtest(test_v2_pipeline_GetInEdgesProcessor processors/GetInEdgesProcessorTest.cpp)
add_v2_pipeline_gtest(test_v2_pipeline_GetEdgesProcessor processors/GetEdgesProcessorTest.cpp)
add_v2_pipeline_gtest(test_v2_pipeline_ProjectionProcessor processors/ProjectionProcessorTest.cpp)
add_v2_pipeline_gtest(test_v2_pipeline_SkipLimitProcessor processors/SkipLimitProcessorTest.cpp)
add_v2_pipeline_gtest(test_v2_pipeline_CartesianProductProcessor processors/CartesianProductProcessorTest.cpp)
