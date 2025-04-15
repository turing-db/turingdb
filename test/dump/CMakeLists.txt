function(add_db_system_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_dump_s
                turing_test_utils_s)
    gtest_discover_tests(${exe_name}
        DISCOVERY_TIMEOUT 10
        DISCOVER_MODE PRE_TEST)
endfunction()

add_db_system_gtest(test_dump_propertycontainerdumper PropertyContainerDumperTest.cpp)
