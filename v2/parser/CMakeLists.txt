flex_target(cypher_lexer ${CMAKE_CURRENT_SOURCE_DIR}/CypherLexer.l
            ${CMAKE_CURRENT_BINARY_DIR}/GeneratedCypherLexer.cpp
            DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/GeneratedCypherLexer.h)

bison_target(cypher_parser ${CMAKE_CURRENT_SOURCE_DIR}/CypherParser.y
             ${CMAKE_CURRENT_BINARY_DIR}/GeneratedCypherParser.cpp
             DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/GeneratedCypherParser.h
             COMPILE_FLAGS "-d -v -Wcounterexamples")

add_flex_bison_dependency(cypher_lexer cypher_parser)

set(parser_sources
    YCypherScanner.cpp
    CypherParser.cpp
    ParserException.cpp
    ${BISON_cypher_parser_OUTPUTS}
    ${FLEX_cypher_lexer_OUTPUTS})

set_source_files_properties(${CMAKE_CURRENT_BINARY_DIR}/GeneratedCypherParser.cpp 
    PROPERTIES
    COMPILE_FLAGS -Wno-unused-but-set-variable)

add_library(turing_db_v2_parser_s STATIC ${parser_sources})

target_include_directories(turing_db_v2_parser_s
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(turing_db_v2_parser_s PUBLIC
    turing_common_s
    turing_db_v2_ast_s
    range-v3)

