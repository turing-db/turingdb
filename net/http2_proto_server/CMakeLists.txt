set(http2_proto_server_sources
    H2ConnectionState.cpp
    H2Parser.cpp
    H2Writer.cpp)

add_library(turing_db_http2_proto_server_s STATIC ${http2_proto_server_sources})

target_include_directories(turing_db_http2_proto_server_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_http2_proto_server_s
    PUBLIC
    turing_db_tcp_server_s
    turing_db_turing_proto_server_s
    turing_db_proto_common_s
    turing_db_storage_s
    turing_db_net_encoders
    turing_common_s
    nghttp2::nghttp2_static)
