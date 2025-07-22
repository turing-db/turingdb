set(ast_sources
    CypherAST.cpp
    CypherASTDumper.cpp
    ASTException.cpp)

add_library(turing_db_v2_ast_s STATIC ${ast_sources})

target_include_directories(turing_db_v2_ast_s
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_v2_ast_s PRIVATE
    turing_common_s
    range-v3)

