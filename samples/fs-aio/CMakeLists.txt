set(SAMPLE_NAME fs-aio)
set(SOURCES     main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE turing_db2_core_s
            turing_fs_s)
