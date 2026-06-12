set(ir_lowering_sources
    DBLowering.cpp)

add_library(turing_db_ir_lowering_s STATIC ${ir_lowering_sources})

target_link_libraries(turing_db_ir_lowering_s
    PUBLIC
        turing_db_ir_dbdialect_s
        turing_db_ir_nldialect_s

    PRIVATE
        turing_db_ir_common_s)

target_include_directories(turing_db_ir_lowering_s PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR})

target_include_directories(turing_db_ir_lowering_s PRIVATE
    ${MLIR_INCLUDE_DIRS}
    ${LLVM_INCLUDE_DIRS})
