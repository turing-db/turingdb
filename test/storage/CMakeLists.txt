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
add_storage_tests(test_storage_dump_graphloader dump/GraphLoaderTest.cpp)
add_storage_tests(test_storage_dump_stringindexloader dump/StringIndexLoaderTest.cpp)
add_storage_tests(test_storage_dump_loadcommit dump/LoadCommitDataTest.cpp)
add_storage_tests(test_storage_dump_embedding_graph dump/EmbeddingGraphDumpLoadTest.cpp)

add_turing_test(test_storage_dump_propertycontainerparquet dump/parquet/PropertyContainerParquetTest.cpp)
target_link_libraries(test_storage_dump_propertycontainerparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_nodecontainerparquet dump/parquet/NodeContainerParquetTest.cpp)
target_link_libraries(test_storage_dump_nodecontainerparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_edgecontainerparquet dump/parquet/EdgeContainerParquetTest.cpp)
target_link_libraries(test_storage_dump_edgecontainerparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_edgeindexerparquet dump/parquet/EdgeIndexerParquetTest.cpp)
target_link_libraries(test_storage_dump_edgeindexerparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_propertyindexerparquet dump/parquet/PropertyIndexerParquetTest.cpp)
target_link_libraries(test_storage_dump_propertyindexerparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_stringindexerparquet dump/parquet/StringPropertyIndexerParquetTest.cpp)
target_link_libraries(test_storage_dump_stringindexerparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_datapartparquet dump/parquet/DataPartParquetTest.cpp)
target_link_libraries(test_storage_dump_datapartparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_graphmetadataparquet dump/parquet/GraphMetadataParquetTest.cpp)
target_link_libraries(test_storage_dump_graphmetadataparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_commitjournalparquet dump/parquet/CommitJournalParquetTest.cpp)
target_link_libraries(test_storage_dump_commitjournalparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_tombstonesparquet dump/parquet/TombstonesParquetTest.cpp)
target_link_libraries(test_storage_dump_tombstonesparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_turing_test(test_storage_dump_commitmetadataparquet dump/parquet/CommitMetaDataParquetTest.cpp)
target_link_libraries(test_storage_dump_commitmetadataparquet PRIVATE
        turing_common_s
        turing_db_storage_s
        turing_db_examples_s
        turing_db_jobs_s
        turing_db_storage_dump_parquet_s)

add_storage_tests(test_storage_columns_dispatcher ColumnDispatcherTest.cpp)
add_storage_tests(test_storage_objectmap ObjectMapTest.cpp)

