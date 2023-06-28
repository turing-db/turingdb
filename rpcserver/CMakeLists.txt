
set(rpcserver_sources
        RPCServerConfig.cpp
        RPCServer.cpp)

add_library(turing_rpc_server_s STATIC
    ${rpcserver_sources})

target_include_directories(turing_rpc_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_rpc_server_s PRIVATE
    turing_log_s
    gRPC::grpc++)

target_include_directories(turing_rpc_server_s PRIVATE ${BIO_SOURCE_DIR}/messages)
