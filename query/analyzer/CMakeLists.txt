set(analyzer_sources
    CypherAnalyzer.cpp
    ExprAnalyzer.cpp
    ReadStmtAnalyzer.cpp
    WriteStmtAnalyzer.cpp
    AnalyzeException.cpp)

add_library(turing_db_analyzer_s STATIC ${analyzer_sources})

target_include_directories(turing_db_analyzer_s
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_analyzer_s
    PRIVATE turing_common_s
            turing_db_parser_s
            turing_db_ast_s
            turing_db_storage_s
    PUBLIC turing_db_query_common_s)

