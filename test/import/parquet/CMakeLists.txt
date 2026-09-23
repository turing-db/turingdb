add_turing_test(test_import_parquet ParquetImporterTest.cpp)

target_compile_definitions(test_import_parquet
    PRIVATE PARQUET_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(test_import_parquet
    PRIVATE turing_db_s
            turing_testenv_s)

add_turing_test(test_import_parquet_timestamp_range ParquetTimestampRangeTest.cpp)

target_compile_definitions(test_import_parquet_timestamp_range
    PRIVATE PARQUET_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(test_import_parquet_timestamp_range
    PRIVATE turing_db_s
            turing_testenv_s)

add_turing_test(test_import_parquet_property_type_clash ParquetPropertyTypeClashTest.cpp)

target_compile_definitions(test_import_parquet_property_type_clash
    PRIVATE PARQUET_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

target_link_libraries(test_import_parquet_property_type_clash
    PRIVATE turing_db_s
            turing_testenv_s)
