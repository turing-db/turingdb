set(SAMPLE_NAME v2)
set(SOURCES
    main.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
  PRIVATE  turing_db_system_s
           turing_db_memory_s
           turing_db_examples_s
           turing_db_v2_parser_s
           turing_db_v2_analyzer_s
           turing_db_v2_plan_s
           turing_db_v2_pipeline_s)
