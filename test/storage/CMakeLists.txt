function(add_storage_tests exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE
            turing_common_s
            turing_db_storage_s
            turing_db_examples_s
            turing_db_jobs_s)
endfunction()

add_storage_tests(test_storage_stringcontainer StringContainerTest.cpp)
add_storage_tests(test_storage_embeddingbucket EmbeddingBucketTest.cpp)
add_storage_tests(test_storage_embeddingcontainer EmbeddingContainerTest.cpp)
add_storage_tests(test_storage_embeddingpropertycontainer EmbeddingPropertyContainerTest.cpp)
add_storage_tests(test_storage_labelset LabelSetTest.cpp)
add_storage_tests(test_storage_labelsetmap LabelSetMapTest.cpp)
add_storage_tests(test_storage_labelmap LabelMapTest.cpp)
add_storage_tests(test_storage_edgetypemap EdgeTypeMapTest.cpp)
add_storage_tests(test_storage_propertytypemap PropertyTypeMapTest.cpp)
add_storage_tests(test_storage_dataframe DataframeTest.cpp)
#add_storage_tests(test_storage_nodecontainer NodeContainerTest.cpp)
add_storage_tests(test_storage_edgecontainer EdgeContainerTest.cpp)
#add_storage_tests(test_storage_edgeindexer EdgeIndexerTest.cpp)
add_storage_tests(test_storage_comparator ComparatorTest.cpp)
add_storage_tests(test_storage_arcmanager ArcManagerTest.cpp)
add_storage_tests(test_storage_graphwriter GraphWriterTest.cpp)

add_storage_tests(test_storage_iterator iterators/IteratorsTest.cpp)
add_storage_tests(test_storage_scan_nodes_iterator iterators/ScanNodesIteratorTest.cpp)
add_storage_tests(test_storage_metadatarebaser versioning/MetadataRebaserTest.cpp)
add_storage_tests(test_storage_datapartmerger versioning/DatapartMergerTest.cpp)

add_storage_tests(test_storage_stringindex StringIndexTest.cpp)

add_storage_tests(test_storage_dump_propertycontainerdumper dump/PropertyContainerDumperTest.cpp)
add_storage_tests(test_storage_dump_propertydumpformat dump/PropertyDumpFormatTest.cpp)
add_storage_tests(test_storage_dump_edgecontainerdumpformat dump/EdgeContainerDumpFormatTest.cpp)
add_storage_tests(test_storage_dump_graphloader dump/GraphLoaderTest.cpp)
add_storage_tests(test_storage_dump_stringindexloader dump/StringIndexLoaderTest.cpp)
add_storage_tests(test_storage_dump_loadcommit dump/LoadCommitDataTest.cpp)
add_storage_tests(test_storage_dump_embedding_graph dump/EmbeddingGraphDumpLoadTest.cpp)

add_storage_tests(test_storage_columns_dispatcher ColumnDispatcherTest.cpp)
add_storage_tests(test_storage_objectmap ObjectMapTest.cpp)

# Microbenchmark (Google Benchmark): AdaptiveRadixTree vs std::unordered_map.
# Not a unit test, so not registered with ctest. Build and run it directly:
#   make bench_storage_adaptiveradixtree && ./test/storage/bench_storage_adaptiveradixtree
add_executable(bench_storage_adaptiveradixtree AdaptiveRadixTreeBenchmark.cpp)
target_link_libraries(bench_storage_adaptiveradixtree PRIVATE
        benchmark::benchmark
        turing_db_storage_s
        turing_common_s)

# Versioned "read as of a commit" benchmark: a single ART (per-commit roots) vs a CommitHash-tagged
# multiversion hash over a dozen committed versions. Build and run it directly:
#   make bench_storage_adaptiveradixtree_versioned && ./test/storage/bench_storage_adaptiveradixtree_versioned
add_executable(bench_storage_adaptiveradixtree_versioned AdaptiveRadixTreeVersionedBenchmark.cpp)
target_link_libraries(bench_storage_adaptiveradixtree_versioned PRIVATE
        benchmark::benchmark
        turing_db_storage_s
        turing_common_s)

