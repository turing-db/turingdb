function(add_db_serialisation_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE 
                turing_db_s
                turing_db_storage_s
                turing_db_memory_s
                turing_db_examples_s
                turing_testenv_s)
endfunction()

add_db_serialisation_gtest(test_stringindex_serial StringIndexSerialisationTest.cpp)
add_db_serialisation_gtest(test_simplegraph_serial SimpleGraphSerialisationTest.cpp)
add_db_serialisation_gtest(test_journal_serial CommitJournalSerialisationTest.cpp)
add_db_serialisation_gtest(test_tombstone_serial TombstoneSerialisationTest.cpp)
