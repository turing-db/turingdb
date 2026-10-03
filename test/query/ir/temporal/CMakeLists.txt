add_turing_test(test_query_ir_map_datetime_property MapDateTimePropertyTest.cpp)
target_link_libraries(test_query_ir_map_datetime_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_map_datetime_property PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_datetime_property DateTimePropertyTest.cpp)
target_link_libraries(test_query_ir_datetime_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_datetime_property PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_duration_of_unwound_count DurationOfUnwoundCountTest.cpp)
target_link_libraries(test_query_ir_duration_of_unwound_count PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_datetime_cell_comparison DateTimeCellComparisonTest.cpp)
target_link_libraries(test_query_ir_datetime_cell_comparison PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_duration_cell_comparison DurationCellComparisonTest.cpp)
target_link_libraries(test_query_ir_duration_cell_comparison PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_temporal_value_order TemporalValueOrderTest.cpp)
target_link_libraries(test_query_ir_temporal_value_order PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_duration_property DurationPropertyTest.cpp)
target_link_libraries(test_query_ir_duration_property PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)

add_turing_test(test_query_ir_datetime_functions DateTimeFunctionsTest.cpp)
target_link_libraries(test_query_ir_datetime_functions PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_datetime_functions PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_duration_component DurationComponentQueryTest.cpp)
target_link_libraries(test_query_ir_duration_component PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_duration_component PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_duration_component_cell DurationComponentCellTest.cpp)
target_link_libraries(test_query_ir_duration_component_cell PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_duration_component_cell PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_datetime_component DateTimeComponentQueryTest.cpp)
target_link_libraries(test_query_ir_datetime_component PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_datetime_component PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_datetime_component_cell DateTimeComponentCellTest.cpp)
target_link_libraries(test_query_ir_datetime_component_cell PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_datetime_component_cell PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_datetime_component_ordering DateTimeComponentOrderingTest.cpp)
target_link_libraries(test_query_ir_datetime_component_ordering PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_datetime_component_ordering PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_datetime_ordering DateTimeOrderingTest.cpp)
target_link_libraries(test_query_ir_datetime_ordering PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_ir_test_rows_s)
target_include_directories(test_query_ir_datetime_ordering PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_load_jsonl_datetime LoadJsonlDateTimeTest.cpp)
target_sources(test_query_ir_load_jsonl_datetime PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_jsonl_datetime PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_jsonl_datetime PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_load_jsonl_datetime_null LoadJsonlDateTimeNullTest.cpp)
target_sources(test_query_ir_load_jsonl_datetime_null PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_jsonl_datetime_null PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_jsonl_datetime_null PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_temporal_arithmetic TemporalArithmeticTest.cpp)
target_link_libraries(test_query_ir_temporal_arithmetic PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_temporal_list_element_arithmetic TemporalListElementArithmeticTest.cpp)
target_link_libraries(test_query_ir_temporal_list_element_arithmetic PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_temporal_mixed_list_element_arithmetic TemporalMixedListElementArithmeticTest.cpp)
target_link_libraries(test_query_ir_temporal_mixed_list_element_arithmetic PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_arithmetic DurationArithmeticTest.cpp)
target_link_libraries(test_query_ir_duration_arithmetic PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_of_map DurationOfMapTest.cpp)
target_link_libraries(test_query_ir_duration_of_map PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_of_map_filter DurationOfMapFilterTest.cpp)
target_link_libraries(test_query_ir_duration_of_map_filter PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_of_map_null_count DurationOfMapNullCountTest.cpp)
target_link_libraries(test_query_ir_duration_of_map_null_count PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_of_unwound_map DurationOfUnwoundMapTest.cpp)
target_link_libraries(test_query_ir_duration_of_unwound_map PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_of_map_null_unknown_unit DurationOfMapNullUnknownUnitTest.cpp)
target_link_libraries(test_query_ir_duration_of_map_null_unknown_unit PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_of_mixed_list_item DurationOfMixedListItemTest.cpp)
target_link_libraries(test_query_ir_duration_of_mixed_list_item PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_scaled_by_a_double DurationScaledByADoubleTest.cpp)
target_link_libraries(test_query_ir_duration_scaled_by_a_double PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_duration_cell_dividend DurationCellDividendTest.cpp)
target_link_libraries(test_query_ir_duration_cell_dividend PRIVATE turing_ir_test_write_query_s)

add_turing_test(test_query_ir_load_jsonl_duration LoadJsonlDurationTest.cpp)
target_sources(test_query_ir_load_jsonl_duration PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_jsonl_duration PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_jsonl_duration PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_load_parquet_duration LoadParquetDurationTest.cpp)
target_sources(test_query_ir_load_parquet_duration PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_parquet_duration PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_parquet_duration PRIVATE ${IR_TEST_DIR})
target_compile_definitions(test_query_ir_load_parquet_duration
    PRIVATE PARQUET_TEST_DATA_DIR="${CMAKE_SOURCE_DIR}/test/import/parquet")

add_turing_test(test_query_ir_load_jsonl_embedding_duration_clash LoadJsonlEmbeddingDurationClashTest.cpp)
target_sources(test_query_ir_load_jsonl_embedding_duration_clash PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_jsonl_embedding_duration_clash PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_jsonl_embedding_duration_clash PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_parquet_duration_logical_type ParquetDurationLogicalTypeTest.cpp)
target_sources(test_query_ir_parquet_duration_logical_type PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_parquet_duration_logical_type PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_parquet_duration_logical_type PRIVATE ${IR_TEST_DIR})
target_compile_definitions(test_query_ir_parquet_duration_logical_type
    PRIVATE PARQUET_TEST_DATA_DIR="${CMAKE_SOURCE_DIR}/test/import/parquet")

add_turing_test(test_query_ir_temporal_cell_arithmetic TemporalCellArithmeticTest.cpp)
target_link_libraries(test_query_ir_temporal_cell_arithmetic PRIVATE turing_ir_test_write_query_s)
