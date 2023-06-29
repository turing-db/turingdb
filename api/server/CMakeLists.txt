
set(apiserver_sources
        APIServiceImpl.cpp
        APIServer.cpp)

set(proto_schema ${TURING_API_PROTO_DIR}/APIService.proto)
set(proto_srcs ${CMAKE_CURRENT_BINARY_DIR}/APIService.pb.cc)
set(proto_headers ${CMAKE_CURRENT_BINARY_DIR}/APIService.pb.h)
set(proto_grpc_srcs ${CMAKE_CURRENT_BINARY_DIR}/APIService.grpc.pb.cc)
set(proto_grpc_headers ${CMAKE_CURRENT_BINARY_DIR}/APIService.grpc.pb.h)

# Generate grpc cpp files
add_custom_command(
    OUTPUT ${proto_srcs} ${proto_headers} ${proto_grpc_srcs} ${proto_grpc_headers}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --grpc_out ${CMAKE_CURRENT_BINARY_DIR}
        --cpp_out ${CMAKE_CURRENT_BINARY_DIR}
        -I ${TURING_API_PROTO_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${proto_schema}
    DEPENDS ${proto_schema})

add_library(turing_api_server_s STATIC
    ${proto_srcs}
    ${proto_grpc_srcs}
    ${apiserver_sources})

target_include_directories(turing_api_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(turing_api_server_s PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(turing_api_server_s PRIVATE
    turing_rpc_server_s
    turing_log_s
    gRPC::grpc++)

target_include_directories(turing_api_server_s PRIVATE ${BIO_SOURCE_DIR}/messages)
