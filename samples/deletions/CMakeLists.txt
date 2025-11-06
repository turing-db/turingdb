set(SAMPLE_NAME deletions)
set(SOURCES main.cpp DeleteTest.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
  turing_testenv_s
  turing_db_examples_s
)
