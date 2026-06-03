set(SAMPLE_NAME parquet-roundtrip)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_common_s
    turing_db_storage_s
    turing_db_storage_dump_parquet_s
    turing_db_jobs_s
)
