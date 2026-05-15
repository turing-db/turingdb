set(http2_client_sources
    H2Client.cpp)

add_library(turing_db_http2_client_s STATIC ${http2_client_sources})

target_include_directories(turing_db_http2_client_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_http2_client_s
    PUBLIC
    turing_db_proto_client_s
    turing_db_proto_common_s
    turing_db_storage_s
    turing_db_memory_s
    turing_common_s
    nghttp2::nghttp2_static)
