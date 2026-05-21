set(SAMPLE_NAME ir)
set(SOURCES
    main.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

configure_file(run.sh ${CMAKE_CURRENT_BINARY_DIR}/run.sh
               FILE_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE
               COPYONLY)

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_system_s
    turing_db_memory_s
    turing_db_parser_s
    turing_db_analyzer_s
    turing_db_examples_s
    turing_db_ast_s
    turing_db_plan_s
    turing_db_query_common_s
    turing_db_s
)
