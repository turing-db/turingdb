set(ir_interpreter_sources
    NLProgram.cpp
    NLOutputSink.cpp
    NLInterpreter.cpp
    NLTranslator.cpp)

add_library(turing_db_ir_interpreter_s STATIC ${ir_interpreter_sources})

target_link_libraries(turing_db_ir_interpreter_s
    PUBLIC
        turing_db_ir_nldialect_s
        turing_db_storage_s
        turing_common_s

    PRIVATE
        turing_db_ir_common_s)

target_include_directories(turing_db_ir_interpreter_s PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR})
