set(SAMPLE_NAME parser2)
set(SOURCES
    main.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
PRIVATE 
    turing_db_query_AST_s
    turing_db_system_s
    turing_db_memory_s
    turing_db_v2_parser_s
    turing_db_v2_plan_s
    turing_db_examples_s
    turing_db_s)
