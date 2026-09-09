set(SAMPLE_NAME cartesian_bench)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_s
    turing_db_interpreter_v3_s
    turing_db_memory_s
    turing_db_ir_interpreter_s
    turing_db_system_s
    turing_db_storage_s
    turing_db_io_fs_s
    turing_common_s
    argparse
    spdlog
)
