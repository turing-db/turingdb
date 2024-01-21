
set(dbserver_sources
        DBServerConfig.cpp
        DBServiceImpl.cpp
        DBServer.cpp)

add_library(turing_db_server_s STATIC
    ${dbserver_sources})

target_include_directories(turing_db_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_server_s PRIVATE
    turing_log_s
    turing_db_core_s
    turing_db_proto_s
    turing_db_dumper_s
    turing_db_filter_s)

target_link_libraries(turing_db_server_s PUBLIC turing_rpc_server_s)

target_include_directories(turing_db_server_s PRIVATE ${BIO_SOURCE_DIR}/messages)
