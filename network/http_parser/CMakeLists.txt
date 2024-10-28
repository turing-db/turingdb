set(http_parser_sources)

add_library(turing_http_parser_s INTERFACE ${http_parser_sources})

target_include_directories(turing_http_parser_s
  INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_http_parser_s
  INTERFACE spdlog::spdlog
            turing_log_s
            common_s)
