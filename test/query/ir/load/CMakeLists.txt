# LOAD PARQUET imports the split-export fixture pair the importer's own tests use, and
# reads the graph it built back through the engine, so the rows come out as text.
add_turing_test(test_query_ir_load_parquet_v3 LoadParquetV3Test.cpp)
target_sources(test_query_ir_load_parquet_v3 PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_parquet_v3 PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_parquet_v3 PRIVATE ${IR_TEST_DIR})
target_compile_definitions(test_query_ir_load_parquet_v3
    PRIVATE PARQUET_TEST_DATA_DIR="${CMAKE_SOURCE_DIR}/test/import/parquet")

# LOAD CSV reads a file out of the data directory, which the system manager's config
# names, so the load tests drive the whole engine the way the search tests do.
add_turing_test(test_query_ir_load_csv_source LoadCSVSourceTest.cpp)
target_sources(test_query_ir_load_csv_source PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_csv_source PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_csv_source PRIVATE ${IR_TEST_DIR})

# The join cases cross a load with the SimpleGraph fixture, so they need the graph the
# search composition cases do.
add_turing_test(test_query_ir_load_csv_match LoadCSVMatchTest.cpp)
target_sources(test_query_ir_load_csv_match PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_csv_match PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_csv_match PRIVATE ${IR_TEST_DIR})

# The import cases run their statements inside a change and submit it, so they need the
# change management the system manager holds.
add_turing_test(test_query_ir_load_csv_write LoadCSVWriteTest.cpp)
target_sources(test_query_ir_load_csv_write PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_csv_write PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_csv_write PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_csv_field_component CSVFieldComponentTest.cpp)
target_link_libraries(test_query_ir_csv_field_component PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_csv_field_component PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_load_csv_optional_match LoadCSVOptionalMatchTest.cpp)
target_sources(test_query_ir_load_csv_optional_match PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_csv_optional_match PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_csv_optional_match PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_load_csv_composition LoadCSVCompositionTest.cpp)
target_sources(test_query_ir_load_csv_composition PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_csv_composition PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_csv_composition PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_load_jsonl_null_property LoadJsonlNullPropertyTest.cpp)
target_sources(test_query_ir_load_jsonl_null_property PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_jsonl_null_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_jsonl_null_property PRIVATE ${IR_TEST_DIR})
