function(add_common_tests exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE
            common_s
            turing_log_s
            gtest_main)

    gtest_discover_tests(${exe_name})
endfunction()

add_common_tests(common_perfstat_tests PerfStatTest.cpp)
