
set(turingdb_sources
    TuringDBTool.cpp
    TuringShell.cpp
    LineNoiseHandle.cpp
    ProtectedLineNoiseSink.cpp
    StartCmd.cpp
    StopCmd.cpp)

add_executable(turingdb ${turingdb_sources})
set_target_properties(turingdb PROPERTIES ENABLE_EXPORTS TRUE)
target_link_libraries(turingdb PUBLIC
    turing_db_s
    turing_db_storage_s
    turing_db_server_s
    turing_db_auth_s
    turing_db_memory_s
    turing_db_proto_client_s
    turing_db_ir_interpreter_s
    turing_db_ir_dbdialect_s
    turing_db_ir_codegen_s
    turing_db_frontend_cypher_s
    turing_db_parser_s
    turing_db_analyzer_s
    turing_db_ast_s
    argparse
    tabulate
    linenoise_s
    termcolor)

install(TARGETS turingdb DESTINATION bin)
