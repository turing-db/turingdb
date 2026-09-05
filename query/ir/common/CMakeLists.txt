
set(ir_common_sources 
    IRModuleInspector.cpp
    ExplainReport.cpp
    IRConstantColumn.cpp
    IRRowAlignment.cpp
    IRException.cpp)

add_library(turing_db_ir_common_s STATIC ${ir_common_sources})

target_link_libraries(turing_db_ir_common_s
    PRIVATE
        MLIRIR
        MLIRFuncDialect

    PUBLIC
        turing_db_query_common_s)

target_include_directories(turing_db_ir_common_s PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR})

target_include_directories(turing_db_ir_common_s PRIVATE 
    ${MLIR_INCLUDE_DIRS}
    ${LLVM_INCLUDE_DIRS})
