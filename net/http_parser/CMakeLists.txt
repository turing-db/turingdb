set(http_parser_sources)

add_library(turing_db_http_parser_s INTERFACE ${http_parser_sources})

target_include_directories(turing_db_http_parser_s
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_http_parser_s
  INTERFACE turing_db_log_s
            turing_db_common_s)
