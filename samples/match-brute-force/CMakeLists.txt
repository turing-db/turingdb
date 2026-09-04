set(SAMPLE_NAME match-brute-force)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_common_s
    turing_db_storage_s
    turing_db_memory_s
    turing_db_system_s
    turing_db_examples_s
    turing_db_interpreter_v3_s
    turing_db_s
)
