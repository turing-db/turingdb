# Fuzzing harnesses. These read from stdin and can be used standalone or with AFL++.
# For instrumented AFL++ builds, use: ./fuzz/run_afl.sh

add_executable(fuzz_query_engine fuzz_query_engine.cpp)
target_link_libraries(fuzz_query_engine PRIVATE
    turing_db_examples_s
    turing_db_s
    turing_testenv_s
    turing_db_parser_s
    turing_db_ast_s
    turing_db_analyzer_s
    turing_db_plan_s
    turing_db_optimizer_s
    turing_db_pipeline_s
    turing_db_interpreter_s
    turing_db_query_common_s)

add_executable(fuzz_csv_parser fuzz_csv_parser.cpp)
target_link_libraries(fuzz_csv_parser PRIVATE
    turing_db_io_csv_s
    turing_db_storage_s
    turing_db_memory_s
    turing_common_s)

add_executable(fuzz_gml_importer fuzz_gml_importer.cpp)
target_link_libraries(fuzz_gml_importer PRIVATE
    turing_db_import_gml_s
    turing_db_storage_s
    turing_db_jobs_s
    turing_common_s)

add_executable(fuzz_http_parser fuzz_http_parser.cpp)
target_link_libraries(fuzz_http_parser PRIVATE
    turing_db_http_common_s
    turing_db_http_parser_s
    turing_common_s)
