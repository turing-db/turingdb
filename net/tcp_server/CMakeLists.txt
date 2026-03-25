set(tcp_server_sources
        TCPServer.cpp
        TCPConnection.cpp
        TCPConnectionStorage.cpp
        TCPListener.cpp
        TCPConnectionManager.cpp)

add_library(turing_db_tcp_server_s STATIC ${tcp_server_sources})

target_include_directories(turing_db_tcp_server_s
  PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_tcp_server_s
  PUBLIC  turing_db_tcp_common_s
          turing_common_s)
