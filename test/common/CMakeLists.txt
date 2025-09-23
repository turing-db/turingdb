function(add_common_tests exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE
            turing_test_utils_s
            turing_common_s
            turing_db_log_s)

    gtest_discover_tests(${exe_name}
        DISCOVERY_TIMEOUT 10
        DISCOVER_MODE PRE_TEST)
endfunction()

add_common_tests(test_common_perfstat PerfStatTest.cpp)

add_common_tests(test_common_smallvector SmallVectorTest.cpp)
