function(add_storage_tests exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE
            turing_db_storage_s
            turing_common_s
            turing_db_jobs_s
            turing_test_utils_s
            gtest_main)

    gtest_discover_tests(${exe_name}
        DISCOVERY_TIMEOUT 10
        DISCOVER_MODE PRE_TEST)
endfunction()

add_storage_tests(test_storage_stringcontainer StringContainerTest.cpp)
add_storage_tests(test_storage_labelset LabelSetTest.cpp)
add_storage_tests(test_storage_labelsetmap LabelSetMapTest.cpp)
add_storage_tests(test_storage_labelmap LabelMapTest.cpp)
add_storage_tests(test_storage_edgetypemap EdgeTypeMapTest.cpp)
add_storage_tests(test_storage_propertytypemap PropertyTypeMapTest.cpp)
#add_storage_tests(test_storage_nodecontainer NodeContainerTest.cpp)
add_storage_tests(test_storage_edgecontainer EdgeContainerTest.cpp)
#add_storage_tests(test_storage_edgeindexer EdgeIndexerTest.cpp)
add_storage_tests(test_storage_comparator ComparatorTest.cpp)
add_storage_tests(test_storage_arcmanager ArcManagerTest.cpp)

add_storage_tests(test_storage_iterator iterators/IteratorsTest.cpp)
add_storage_tests(test_storage_scan_nodes_iterator iterators/ScanNodesIteratorTest.cpp)
add_storage_tests(test_storage_metadatarebaser versioning/MetadataRebaserTest.cpp)

add_storage_tests(test_storage_stringindex StringIndexTest.cpp)
