function(add_db_system_tests exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_storage_s
                turing_db_system_s
                turing_testenv_s
                turing_testutils_s)
endfunction()

add_db_system_tests(test_system SystemTest.cpp)
