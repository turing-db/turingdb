
set(turingdb_sources
    TuringDBTool.cpp
    TuringShell.cpp)

add_executable(turingdb ${turingdb_sources})
target_link_libraries(turingdb PUBLIC
    turing_db_s
    turing_db_storage_s
    turing_db_server_s
    turing_db_memory_s
    argparse
    tabulate
    linenoise_s
    termcolor)

install(TARGETS turingdb DESTINATION bin)
