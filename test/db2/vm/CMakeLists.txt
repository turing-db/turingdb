function(add_db2_vm_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_vm_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_vm_gtest(db2_vm_dataregistrymanager_test DataRegistryManagerTest.cpp)
add_db2_vm_gtest(db2_vm_filter_test FilterTest.cpp)
