set(tcp_common_sources
    BaseConnectionState.cpp
    NetBuffer.cpp
    NetException.cpp
    SocketUtils.cpp)

add_library(turing_db_tcp_common_s STATIC ${tcp_common_sources})

target_include_directories(turing_db_tcp_common_s
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_tcp_common_s
  PRIVATE turing_common_s)
