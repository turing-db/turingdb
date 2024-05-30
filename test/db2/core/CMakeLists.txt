function(add_db2_core_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_core_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_core_gtest(db2_core_labelset_test LabelSetTest.cpp)
add_db2_core_gtest(db2_core_labelmap_test LabelMapTest.cpp)
add_db2_core_gtest(db2_core_nodecontainer_test NodeContainerTest.cpp)
add_db2_core_gtest(db2_core_edgecontainer_test EdgeContainerTest.cpp)
add_db2_core_gtest(db2_core_edgeindexer_test EdgeIndexerTest.cpp)
add_db2_core_gtest(db2_core_concurrent_writer_test ConcurrentWriterTest.cpp)
add_db2_core_gtest(db2_core_iterator_test iterators/IteratorsTest.cpp)
add_db2_core_gtest(db2_core_scan_nodes_iterator_test iterators/ScanNodesIteratorTest.cpp)
