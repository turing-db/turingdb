function(add_db2_server_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_core_s
                turing_db2_server_s
                turing_http_client_s
                turing_samples_pipe_utilslib_s
                nlohmann_json
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_server_gtest(db2_server_test ServerTest.cpp)
