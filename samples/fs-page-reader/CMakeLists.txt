set(SAMPLE_NAME fs-page-reader)
set(SOURCES     main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE common_s
            turing_log_s
            turing_fs_s)
