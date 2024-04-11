function(add_db2_pipeline_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file} TestUtils.cpp)
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_core_s
                turing_db2_pipeline_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_pipeline_gtest(db2_pipeline_test PipelineTest.cpp)
add_db2_pipeline_gtest(db2_scan_nodes_processor_test ScanNodesProcessorTest.cpp)
