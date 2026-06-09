set(SAMPLE_NAME mlir-as)

set(SOURCES
    main.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    turing_db_ir_assembler_s
    turing_db_ir_common_s
    turing_db_ir_dbdialect_s
    turing_db_io_fs_s
    turing_common_s
    argparse
    spdlog
)

target_include_directories(${SAMPLE_NAME} PRIVATE ${MLIR_INCLUDE_DIRS} ${LLVM_INCLUDE_DIRS})
