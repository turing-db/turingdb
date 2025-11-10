set(SAMPLE_NAME column-switch)
set (SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE turing_common_s
            turing_db_s
            turing_db_storage_s
)

