set(SAMPLE_NAME jsonl-import)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_storage_s
    turing_db_system_s
    turing_db_examples_s
    turing_db_report_s
    turing_db_import_jsonl_s
    turing_db_s
)
