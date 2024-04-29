function(add_db2_core_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_core_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_core_gtest(db2_core_data_buffer_test DataBufferTest.cpp)
add_db2_core_gtest(db2_core_datapart_test DataPartTest.cpp)
add_db2_core_gtest(db2_core_labelset_test LabelSetTest.cpp)
add_db2_core_gtest(db2_core_edgecontainer_test EdgeContainerTest.cpp)
add_db2_core_gtest(db2_core_propertycontainer_test PropertyContainerTest.cpp)
add_db2_core_gtest(db2_core_mandatorypropertystorage_test MandatoryPropertyStorageTest.cpp)
add_db2_core_gtest(db2_core_optionalpropertystorage_test OptionalPropertyStorageTest.cpp)
add_db2_core_gtest(db2_core_iterator_test iterators/IteratorsTest.cpp)
add_db2_core_gtest(db2_core_concurrent_write_test iterators/ConcurrentWriterTest.cpp)
add_db2_core_gtest(db2_core_scan_nodes_iterator_test iterators/ScanNodesIteratorTest.cpp)
