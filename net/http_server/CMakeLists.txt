
set(http_server_sources
        Utils.cpp
        HTTPServer.cpp
        TCPConnection.cpp
        TCPConnectionStorage.cpp
        TCPListener.cpp
        TCPConnectionManager.cpp)

add_library(turing_db_http_server_s STATIC ${http_server_sources})

target_include_directories(turing_db_http_server_s
  PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_http_server_s
  PUBLIC  turing_db_http_common_s
          turing_db_http_parser_s
          turing_db_log_s
          turing_db_common_s)
