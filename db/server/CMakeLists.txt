
set(dbserver_sources
        DBServerConfig.cpp
        DBServiceImpl.cpp
        DBServer.cpp)

set(proto_schema ${TURING_DB_PROTO_DIR}/DBService.proto)
set(proto_srcs ${CMAKE_CURRENT_BINARY_DIR}/DBService.pb.cc)
set(proto_headers ${CMAKE_CURRENT_BINARY_DIR}/DBService.pb.h)
set(proto_grpc_srcs ${CMAKE_CURRENT_BINARY_DIR}/DBService.grpc.pb.cc)
set(proto_grpc_headers ${CMAKE_CURRENT_BINARY_DIR}/DBService.grpc.pb.h)

# Generate grpc cpp files
add_custom_command(
    OUTPUT ${proto_srcs} ${proto_headers} ${proto_grpc_srcs} ${proto_grpc_headers}
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --grpc_out ${CMAKE_CURRENT_BINARY_DIR}
        --cpp_out ${CMAKE_CURRENT_BINARY_DIR}
        -I ${TURING_DB_PROTO_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${proto_schema}
    DEPENDS ${proto_schema})

add_library(turing_db_server_s STATIC
    ${proto_srcs}
    ${proto_grpc_srcs}
    ${dbserver_sources})

target_include_directories(turing_db_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(turing_db_server_s PRIVATE ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(turing_db_server_s PRIVATE
    turing_log_s
    turing_db_core_s
    turing_db_dumper_s
    turing_db_filter_s
    gRPC::grpc++)

target_link_libraries(turing_db_server_s PUBLIC turing_rpc_server_s)

target_include_directories(turing_db_server_s PRIVATE ${BIO_SOURCE_DIR}/messages)
