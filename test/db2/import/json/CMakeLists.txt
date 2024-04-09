function(add_db2_import_json_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file} JobSystem.cpp)
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_import_json_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_import_json_gtest(db2_import_json_test Neo4jJsonParserTest.cpp)

gtest_discover_tests(db2_import_json_test)
