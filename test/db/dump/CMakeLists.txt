function(add_db_dumper_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})

    target_link_libraries(${exe_name} PRIVATE
        bio_import_json_s
        turing_db_core_s
        turing_db_dumper_s
        turing_log_s gtest_main
    )

    gtest_discover_tests(${exe_name})
endfunction()

add_db_dumper_gtest(string_index_loader_tests StringIndexLoaderTest.cpp)
add_db_dumper_gtest(string_index_dumper_tests StringIndexDumperTest.cpp)
add_db_dumper_gtest(db_dumper_tests DBDumperTest.cpp)
add_db_dumper_gtest(db_loader_tests DBLoaderTest.cpp)
