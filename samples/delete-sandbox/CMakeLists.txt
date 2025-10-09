set(SAMPLE_NAME delete-sandbox)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_s
    turing_db_memory_s)
