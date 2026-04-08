set(SAMPLE_NAME random-graph-generator)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_storage_s
    turing_db_system_s
    turing_db_s
)
