
set(turingdb_sources
    TuringDBTool.cpp
    TuringShell.cpp
    ShellTable.cpp
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
    turing_db_interpreter_v3_s
    argparse
    linenoise_s
    termcolor)

install(TARGETS turingdb DESTINATION bin)
