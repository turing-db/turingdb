
add_executable(bytecompiler ByteCompiler.cpp)
target_link_libraries(bytecompiler PRIVATE
    turing_db2_query_AST_s
    turing_db2_query_parser_s
    turing_db2_compiler_s
    tools_common_s
    turing_log_s)
