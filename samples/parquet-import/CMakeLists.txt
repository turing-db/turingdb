set(SAMPLE_NAME parquet-import)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_io_fs_s
    turing_db_io_parquet_s
)
