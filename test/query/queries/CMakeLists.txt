function(add_queries_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db_examples_s
                turing_db_s
                turing_testenv_s)
    target_include_directories(${exe_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties(${exe_name} PROPERTIES ENABLE_EXPORTS TRUE)
endfunction()

add_queries_gtest(test_queries QueriesTest.cpp)
add_queries_gtest(test_filter_predicates FilterPredicatesTest.cpp)
add_queries_gtest(test_delete_queries DeleteQueriesTest.cpp)
add_queries_gtest(test_write_queries WriteQueriesTest.cpp)
add_queries_gtest(test_load_graph LoadGraphTest.cpp)
add_queries_gtest(test_create_graph CreateGraphTest.cpp)
add_queries_gtest(test_change_queries ChangeQueriesTest.cpp)
add_queries_gtest(test_complex_null_predicates ComplexNullPredicatesTest.cpp)
add_queries_gtest(test_conversion_function_null ConversionFunctionNullTest.cpp)
add_queries_gtest(test_multi_hop_pattern MultiHopPatternTest.cpp)
add_queries_gtest(test_auto_create_command AutoCreateCommandTest.cpp)
add_queries_gtest(test_match_create MatchCreateTest.cpp)
add_queries_gtest(test_string_filter StringFilterTest.cpp)
add_queries_gtest(test_join_feature AutoJoinTest.cpp)
add_queries_gtest(test_show_procedures ShowProceduresTest.cpp)
add_queries_gtest(test_call_procedure CallProcedureTest.cpp)
add_queries_gtest(test_call_procedure_chunked_input CallProcedureChunkedInputTest.cpp)
add_queries_gtest(test_shortest_path ShortestPathQueryTest.cpp)
add_queries_gtest(test_incorrect_query IncorrectQueryTest.cpp)
add_queries_gtest(test_load_csv LoadCSVTest.cpp)
add_queries_gtest(test_vector_queries VectorQueriesTest.cpp)
add_queries_gtest(test_embedding_queries EmbeddingQueriesTest.cpp)
add_queries_gtest(test_load_embedding LoadEmbeddingTest.cpp)
# Needs the Parquet writer to produce test inputs; the pipeline links
# turing_db_io_parquet_s privately, so depend on it explicitly here.
target_link_libraries(test_load_embedding PRIVATE turing_db_io_parquet_s)
add_queries_gtest(test_load_commit LoadCommitTest.cpp)

add_queries_gtest(test_greeter_extension GreeterExtensionTest.cpp)
add_queries_gtest(test_show_extensions ShowExtensionsTest.cpp)
add_queries_gtest(test_reactome ReactomeTest.cpp)
add_queries_gtest(test_mixed_predicate_or MixedPredicateOrTest.cpp)
add_queries_gtest(test_unwind_queries UnwindQueriesTest.cpp)
add_queries_gtest(test_aggregate_over_list AggregateOverListTest.cpp)
add_queries_gtest(test_index_queries IndexQueriesTest.cpp)
add_queries_gtest(test_merge_data_parts MergeDataPartsTest.cpp)
add_queries_gtest(test_const_scan_opt ConstScanOptTest.cpp)
target_link_libraries(test_const_scan_opt
    PRIVATE turing_db_parser_s
            turing_db_analyzer_s
            turing_db_ast_s
            turing_db_plan_s
            turing_db_optimizer_s
            turing_db_pipeline_s
            turing_db_memory_s
            turing_db_query_common_s)
