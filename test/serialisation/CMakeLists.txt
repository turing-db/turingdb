function(add_db_serialisation_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE 
                turing_db_s
                turing_db_storage_s
                turing_db_memory_s
                turing_db_examples_s
                gtest_main
                )
endfunction()

add_db_serialisation_gtest(
    test_stringindexserialisation StringIndexSerialisationTest.cpp
    test_simplegraphserialisation SimpleGraphSerialisationTest.cpp)
