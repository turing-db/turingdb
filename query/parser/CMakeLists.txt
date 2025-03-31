flex_target(lexer ${CMAKE_CURRENT_SOURCE_DIR}/lexer.l
            ${CMAKE_CURRENT_BINARY_DIR}/lexer.cpp
            DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/lexer.h)

bison_target(parser ${CMAKE_CURRENT_SOURCE_DIR}/parser.y
             ${CMAKE_CURRENT_BINARY_DIR}/parser.cpp
             COMPILE_FLAGS "-d -v")

add_flex_bison_dependency(lexer parser)

set(parser_sources
        ${BISON_parser_OUTPUTS}
        ${FLEX_lexer_OUTPUTS}
        QueryParser.cpp)

set_source_files_properties(${CMAKE_CURRENT_BINARY_DIR}/parser.cpp 
    PROPERTIES
    COMPILE_FLAGS -Wno-unused-but-set-variable)

add_library(turing_db_query_parser_s STATIC ${parser_sources})
target_include_directories(turing_db_query_parser_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(turing_db_query_parser_s PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(turing_db_query_parser_s PRIVATE
    turing_db_query_AST_s
    turing_common_s
    turing_db_storage_s
)
