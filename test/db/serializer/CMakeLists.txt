function(add_db_serializer_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})

    target_link_libraries(${exe_name} PRIVATE
        turing_db_core_s
        turing_db_serializer_s
        turing_log_s gtest_main
    )

    gtest_discover_tests(${exe_name})
endfunction()

#add_db_serializer_gtest(db_serializer_tests DBSerializerTest.cpp)
add_db_serializer_gtest(string_index_serializer_tests StringIndexSerializerTest.cpp)
