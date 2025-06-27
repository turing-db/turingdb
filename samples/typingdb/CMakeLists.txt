set(SAMPLE_NAME typingdb)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_dump_s
    turing_db_storage_s
    turing_db_system_s
    turing_db_examples_s
    turing_db_report_s
    turing_db_s
    turing_db_tools_common_s)
