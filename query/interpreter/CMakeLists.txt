set(interpreter_sources
    QueryInterpreterV3.cpp)

add_library(turing_db_interpreter_s STATIC ${interpreter_sources})
target_include_directories(turing_db_interpreter_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_interpreter_s
    PUBLIC
        turing_db_ir_exec_s
        turing_db_system_s
        turing_common_s
    PRIVATE
        turing_db_ir_dbdialect_s
        turing_db_ir_common_s
        turing_db_ir_codegen_s
        turing_db_frontend_cypher_s
        turing_db_parser_s
        turing_db_analyzer_s
        turing_db_ast_s)
