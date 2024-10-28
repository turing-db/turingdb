
set(http_server_sources
        Utils.cpp
        Server.cpp
        TCPConnection.cpp
        TCPConnectionStorage.cpp
        TCPListener.cpp
        TCPConnectionManager.cpp)

add_library(turing_http_server_s STATIC ${http_server_sources})

target_include_directories(turing_http_server_s
  PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_http_server_s
  PUBLIC  spdlog::spdlog
          turing_http_parser_s
          turing_log_s
          common_s)
