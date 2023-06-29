
install(DIRECTORY turingapi DESTINATION lib/python
    PATTERN *.swp EXCLUDE)

# Generate gRPC python code
set(proto_schema ${TURING_API_PROTO_DIR}/APIService.proto)
set(proto_schema_out_dir ${CMAKE_CURRENT_BINARY_DIR}/turingapi/proto)
set(proto_schema_out ${proto_schema_out_dir}/APIService.proto)
set(proto_py
    ${proto_schema_out_dir}/APIService_pb2.py
    ${proto_schema_out_dir}/APIService_pb2_grpc.py)

file(COPY ${proto_schema} DESTINATION ${proto_schema_out_dir})

add_custom_command(
    OUTPUT ${proto_py}
    COMMAND python3
    ARGS
        -m grpc_tools.protoc
        -I ${CMAKE_CURRENT_BINARY_DIR}
        --python_out=${CMAKE_CURRENT_BINARY_DIR}
        --pyi_out=${CMAKE_CURRENT_BINARY_DIR}
        --grpc_python_out=${CMAKE_CURRENT_BINARY_DIR}
        ${proto_schema_out}
    DEPENDS ${proto_schema_out})

add_library(turing_api_client_python INTERFACE)
target_sources(turing_api_client_python PUBLIC ${proto_py})

get_target_property(turing_api_client_python_sources turing_api_client_python SOURCES)
install(FILES ${turing_api_client_python_sources} DESTINATION lib/python/turingapi/proto)
