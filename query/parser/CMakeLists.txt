set(parser_sources
    CypherToken.cpp
    CypherLexer.cpp
    CypherGrammar.cpp
    CypherGrammarQueries.cpp
    CypherGrammarClauses.cpp
    CypherGrammarPatterns.cpp
    CypherGrammarExpressions.cpp
    CypherParser.cpp
    ParserException.cpp
    ParserUtils.cpp)

add_library(turing_db_parser_s STATIC ${parser_sources})

target_include_directories(turing_db_parser_s
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_parser_s
    PRIVATE turing_common_s
            turing_db_query_common_s
            turing_db_import_common_s
            turing_vector_s
            range-v3
    PUBLIC  turing_db_ast_s)
