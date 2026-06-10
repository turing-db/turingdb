set(SAMPLE_NAME jsonl-import)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

file(GLOB JSONL_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.jsonl")
file(COPY ${JSONL_FILES} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
file(COPY "run.sh" DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

# run.sh needs to be executable
file(CHMOD ${CMAKE_CURRENT_BINARY_DIR}/run.sh
     PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)


target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_storage_s
    turing_db_system_s
    turing_db_examples_s
    turing_db_report_s
    turing_db_import_jsonl_s
    turing_db_s
)
