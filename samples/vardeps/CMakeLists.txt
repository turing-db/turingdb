set(SAMPLE_NAME vardeps)
set(SOURCES
    main.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

foreach(f run.sh shell.nix asciimermaid.py package.json)
    configure_file(${f} ${CMAKE_CURRENT_BINARY_DIR}/${f} COPYONLY)
endforeach()

# run.sh needs to be executable
file(CHMOD ${CMAKE_CURRENT_BINARY_DIR}/run.sh
     PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

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
