function(add_db2_core_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_core_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_core_gtest(db2_core_temp_data_buffer_test TemporaryDataBufferTest.cpp)
add_db2_core_gtest(db2_core_datapart_test DataPartTest.cpp)
add_db2_core_gtest(db2_core_iterator_test iterators/IteratorsTest.cpp)
#add_db2_core_gtest(db2_core_chunk_reader_test iterators/ChunkReaderTest.cpp)
add_db2_core_gtest(db2_core_scan_nodes_iterator_test iterators/ScanNodesIteratorTest.cpp)
