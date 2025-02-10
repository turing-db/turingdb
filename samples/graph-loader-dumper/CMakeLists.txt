set(SAMPLE_NAME graph-loader-dumper)
set(SOURCES     main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE common_s
            turing_log_s
            turing_db2_import_neo4j_s
            turing_db2_dump_s)
