function(add_vector_tests exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_vector_s
                turing_testutils_s)
endfunction()

add_vector_tests(test_jsonimporter JsonImporterTest.cpp)
