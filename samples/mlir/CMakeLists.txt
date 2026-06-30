set(SAMPLE_NAME mlir)

set(SOURCES
    main.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    turing_db_ir_assembler_s
    turing_db_ir_common_s
    turing_db_ir_dbdialect_s
    turing_db_ir_nldialect_s
    turing_db_ir_storagedialect_s
    turing_db_ir_interpreter_s
    turing_db_storage_s
    turing_db_io_fs_s
    turing_common_s
    turing_db_ast_s
    turing_db_analyzer_s
    turing_db_parser_s
    turing_db_examples_s
    turing_db_system_s
    turing_db_ir_codegen_s
    turing_db_frontend_cypher_s
    argparse
    spdlog
)

target_include_directories(${SAMPLE_NAME} PRIVATE ${MLIR_INCLUDE_DIRS} ${LLVM_INCLUDE_DIRS})

file(GLOB MLIR_FILES "*.mlir")
foreach(MLIR_FILE ${MLIR_FILES})
    configure_file(${MLIR_FILE} ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
endforeach()
