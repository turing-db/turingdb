
set(turingdb_sources TuringDBTool.cpp)

add_executable(turingdb ${turingdb_sources})
target_link_libraries(turingdb PUBLIC
    turing_db_s
    turing_db_tools_common_s
    argparse)

install(TARGETS turingdb DESTINATION bin)
