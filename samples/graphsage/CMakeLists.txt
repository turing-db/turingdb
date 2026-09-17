set(SAMPLE_NAME graphsage)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_common_s
    turing_db_storage_s
    turing_db_jobs_s
    turing_db_examples_s
)
