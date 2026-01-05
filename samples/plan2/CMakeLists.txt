set(SAMPLE_NAME plan2)
set(SOURCES
    plan2.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
PRIVATE 
    turing_db_parser_s
    turing_db_ast_s
    turing_db_analyzer_s
    turing_db_system_s
    turing_db_memory_s
    turing_db_plan_s
    turing_db_examples_s
    turing_db_s)
