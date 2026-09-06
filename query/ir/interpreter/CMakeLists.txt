set(ir_interpreter_sources
    NLProgram.cpp
    NLOutputSink.cpp
    NLExecutionContext.cpp
    NLWriteProperties.cpp
    NLWrittenValues.cpp
    NLExecutor.cpp
    NLMergeExecutor.cpp
    NLMergeWorkingSet.cpp
    NLTranslator.cpp
    NLInterpreter.cpp
    NLSystemContext.cpp
    NLSystemExecutor.cpp
    NLSystemTranslator.cpp
    DBDialectInterpreter.cpp)

add_library(turing_db_ir_interpreter_s STATIC ${ir_interpreter_sources})

target_link_libraries(turing_db_ir_interpreter_s
    PUBLIC
        turing_db_ir_nldialect_s
        turing_db_ir_lowering_s
        turing_db_storage_s
        turing_db_memory_s
        turing_db_procedures_s
        turing_common_s

    PRIVATE
        turing_db_ir_common_s
        turing_db_system_s
        turing_db_io_csv_s
        turing_db_io_parquet_s)

target_include_directories(turing_db_ir_interpreter_s PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR})
