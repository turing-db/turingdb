add_turing_test(test_import_parquet_neo4j Neo4jParquetImporterTest.cpp)

target_compile_definitions(test_import_parquet_neo4j
    PRIVATE PARQUET_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(test_import_parquet_neo4j
    PRIVATE turing_db_s
            turing_testenv_s)
