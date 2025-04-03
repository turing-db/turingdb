set(SAMPLE_NAME graph-loader-dumper)
set(SOURCES     main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE turing_common_s
            turing_db_log_s
            turing_db_import_neo4j_s
            turing_db_examples_s
            turing_db_dump_s)
