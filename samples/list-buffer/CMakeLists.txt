set(SAMPLE_NAME list-buffer)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
  turing_db_examples_s
  turing_db_storage_s
  turing_db_memory_s
)
