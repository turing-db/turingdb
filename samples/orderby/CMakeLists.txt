set(SAMPLE_NAME orderby)
set(SOURCES main.cpp sort.h)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
  turing_db_s
  turing_db_storage_s
)
