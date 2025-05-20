set(SAMPLE_NAME fs-file)
set(SOURCES     main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE 
    turing_common_s
    turing_db_log_s
    turing_db_io_fs_s)
