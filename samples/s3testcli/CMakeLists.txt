set(SAMPLE_NAME s3testcli)
set(SOURCES S3TestCli.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(s3testcli PUBLIC
    turing_db_s
    turing_db_storage_s
    turing_db_server_s
    turing_db_memory_s
    turing_db_tools_common_s
    turing_db_io_s3_s 
    turing_db_io_filecache_s
    argparse
    tabulate
    linenoise_s
    termcolor)
