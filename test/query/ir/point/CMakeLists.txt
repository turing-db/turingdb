add_turing_test(test_query_ir_point_property PointPropertyTest.cpp)
target_include_directories(test_query_ir_point_property PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_point_value_flow PointValueFlowTest.cpp)
target_include_directories(test_query_ir_point_value_flow PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_point_component PointComponentTest.cpp)
target_include_directories(test_query_ir_point_component PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_point_spatial_functions PointSpatialFunctionsTest.cpp)
target_include_directories(test_query_ir_point_spatial_functions PRIVATE ${IR_TEST_DIR})

function(add_point_import_test exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_sources(${exe_name} PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp ${CMAKE_CURRENT_SOURCE_DIR}/PointImportTest.cpp)
    target_include_directories(${exe_name} PRIVATE ${IR_TEST_DIR})
endfunction()

function(add_parquet_point_import_test exe_name cpp_file)
    add_point_import_test(${exe_name} ${cpp_file})
    target_sources(${exe_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/ParquetPointImportTest.cpp)
    target_compile_definitions(${exe_name} PRIVATE PARQUET_TEST_DATA_DIR="${CMAKE_SOURCE_DIR}/test/import/parquet")
endfunction()

add_point_import_test(test_query_ir_load_jsonl_point LoadJsonlPointTest.cpp)
add_point_import_test(test_query_ir_jsonl_point_map JsonlPointMapTest.cpp)

add_parquet_point_import_test(test_query_ir_load_parquet_point LoadParquetPointTest.cpp)
add_parquet_point_import_test(test_query_ir_geoparquet_metadata GeoParquetMetadataTest.cpp)
add_parquet_point_import_test(test_query_ir_parquet_point_srid ParquetPointSRIDTest.cpp)
add_parquet_point_import_test(test_query_ir_parquet_wkb_type ParquetWKBTypeTest.cpp)
add_parquet_point_import_test(test_query_ir_parquet_inline_crs ParquetInlineCRSTest.cpp)

add_turing_test(test_query_ir_point_order_and_aggregate PointOrderAndAggregateTest.cpp)
target_include_directories(test_query_ir_point_order_and_aggregate PRIVATE ${IR_TEST_DIR})
