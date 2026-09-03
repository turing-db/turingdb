set(SAMPLE_NAME scan_bench)
set(SOURCES main.cpp)

set(${SAMPLE_NAME}_EXCLUDE_FROM_CI 1 PARENT_SCOPE)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_db_s
    turing_db_memory_s
    turing_db_ir_common_s
    turing_db_ir_dbdialect_s
    turing_db_ir_nldialect_s
    turing_db_ir_interpreter_s
    turing_db_storage_s
    turing_db_io_fs_s
    turing_common_s
    argparse
    spdlog
)

target_include_directories(${SAMPLE_NAME} PRIVATE ${MLIR_INCLUDE_DIRS} ${LLVM_INCLUDE_DIRS})
