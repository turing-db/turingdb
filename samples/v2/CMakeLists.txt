set(SAMPLE_NAME analyzer2)
set(SOURCES
    main.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
  PRIVATE  turing_db_system_s
           turing_db_memory_s
           turing_db_v2_parser_s
           turing_db_v2_analyzer_s)
