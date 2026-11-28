# The request body and query helpers the tests over the binary protocol share.
add_library(turing_net_test_proto_s STATIC ProtoTestUtils.cpp)
target_link_libraries(turing_net_test_proto_s
    PUBLIC  turing_query_result_formatter_s
            turing_db_proto_client_s
            turing_db_net_encoders
    PRIVATE GTest::gtest)
target_include_directories(turing_net_test_proto_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_turing_test(test_net_http_parser HTTPParserTest.cpp)

add_turing_test(test_net_turing_proto_framing TuringProtoFramingTest.cpp)

add_turing_test(test_net_turing_proto_roundtrip TuringProtoRoundTripTest.cpp)

add_turing_test(test_net_turing_proto_empty_copy TuringProtoEmptyCopyTest.cpp)

add_turing_test(test_net_turing_proto_parameter_writer TuringProtoParameterWriterTest.cpp)

add_turing_test(test_net_turing_proto_parameter_reader TuringProtoParameterReaderTest.cpp)

add_turing_test(test_net_turing_proto_request_body TuringProtoRequestBodyTest.cpp)

add_turing_test(test_net_proto_analyze_error ProtoAnalyzeErrorTest.cpp)

add_turing_test(test_net_turing_client TuringClientTest.cpp)

add_turing_test(test_net_turing_client_timeout TuringClientTimeoutTest.cpp)

add_turing_test(test_net_turing_async_client_connect TuringAsyncClientConnectTest.cpp)

add_turing_test(test_net_value_store_after_thrown_query ValueStoreAfterThrownQueryTest.cpp)
target_sources(test_net_value_store_after_thrown_query PRIVATE ProtoServerTest.cpp TruncatingProxy.cpp)

add_turing_test(test_net_turing_client_caller_store TuringClientCallerStoreTest.cpp)
target_sources(test_net_turing_client_caller_store PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_turing_async_client_queries TuringAsyncClientQueriesTest.cpp)
target_sources(test_net_turing_async_client_queries PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_turing_proto_endtoend TuringProtoEndToEndTest.cpp)

add_turing_test(test_net_turing_proto_empty_result TuringProtoEmptyResultTest.cpp)

add_turing_test(test_net_request_too_large RequestTooLargeTest.cpp)

add_turing_test(test_net_request_status RequestStatusTest.cpp)

add_turing_test(test_net_version_param VersionParamTest.cpp)

add_turing_test(test_net_change_id_param ChangeIDParamTest.cpp)

add_turing_test(test_net_protocol_error_closes_connection ProtocolErrorClosesConnectionTest.cpp)
target_sources(test_net_protocol_error_closes_connection PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_turing_client_buffer TuringClientBufferTest.cpp)
target_sources(test_net_turing_client_buffer PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_turing_proto_parameters TuringProtoParametersTest.cpp)

add_turing_test(test_net_client_after_protocol_error ClientAfterProtocolErrorTest.cpp)
target_sources(test_net_client_after_protocol_error PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_chunk_missing_bytes ChunkMissingBytesTest.cpp)

add_turing_test(test_net_split_error_message SplitErrorMessageTest.cpp)
target_sources(test_net_split_error_message PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_payload_size_limit PayloadSizeLimitTest.cpp)

add_turing_test(test_net_unsent_request_keeps_connection UnsentRequestKeepsConnectionTest.cpp)
target_sources(test_net_unsent_request_keeps_connection PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_response_without_end ResponseWithoutEndTest.cpp)

add_turing_test(test_net_protocol_error_response ProtocolErrorResponseTest.cpp)

add_turing_test(test_net_server_buffer_limit ServerBufferLimitTest.cpp)

add_turing_test(test_net_tcp_server_bound_port TCPServerBoundPortTest.cpp)

add_turing_test(test_net_schema_id_cells_over_the_wire SchemaIDCellsOverTheWireTest.cpp)
target_sources(test_net_schema_id_cells_over_the_wire PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_turing_proto_nullable_window TuringProtoNullableWindowTest.cpp)

add_turing_test(test_net_turing_proto_nullable_buffer TuringProtoNullableBufferTest.cpp)

add_turing_test(test_net_nested_writer_after_throw NestedWriterAfterThrowTest.cpp)

add_turing_test(test_net_request_body_after_refusal RequestBodyAfterRefusalTest.cpp)

add_turing_test(test_net_client_parameters_after_refusal ClientParametersAfterRefusalTest.cpp)
target_sources(test_net_client_parameters_after_refusal PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_aborted_response AbortedResponseTest.cpp)
target_sources(test_net_aborted_response PRIVATE ProtoServerTest.cpp)

add_turing_test(test_net_py_sink_decode PySinkDecodeTest.cpp)
target_sources(test_net_py_sink_decode PRIVATE ProtoServerTest.cpp)
target_link_libraries(test_net_py_sink_decode PRIVATE turing_py_sink_s)

# The Python clients, run in an embedded interpreter
find_package(Python 3.9 REQUIRED COMPONENTS Interpreter OPTIONAL_COMPONENTS Development.Embed)
if (Python_Development.Embed_FOUND)
    execute_process(COMMAND ${Python_EXECUTABLE} -c "import sys; print(sys.base_prefix, end='')"
                    OUTPUT_VARIABLE PYTHON_HOME
                    RESULT_VARIABLE PYTHON_HOME_RESULT)
    if (NOT PYTHON_HOME_RESULT EQUAL 0)
        message(FATAL_ERROR "${Python_EXECUTABLE} did not print its sys.base_prefix")
    endif()

    add_library(turing_test_embedded_python_s STATIC EmbeddedPython.cpp)
    target_compile_definitions(turing_test_embedded_python_s PRIVATE PYTHON_HOME="${PYTHON_HOME}")
    target_link_libraries(turing_test_embedded_python_s PUBLIC Python::Python)

    add_turing_test(test_net_embedded_python EmbeddedPythonTest.cpp)
    target_link_libraries(test_net_embedded_python PRIVATE turing_test_embedded_python_s)

    add_turing_test(test_net_py_result_builder_empty_constant PyResultBuilderEmptyConstantTest.cpp)
    nanobind_set_visibility(test_net_py_result_builder_empty_constant)
    target_sources(test_net_py_result_builder_empty_constant PRIVATE
        ProtoServerTest.cpp
        ${CMAKE_SOURCE_DIR}/python/turingdb/_binary/PyObjectColumn.cpp
        ${CMAKE_SOURCE_DIR}/python/turingdb/_binary/PyResultBuilder.cpp)
    target_link_libraries(test_net_py_result_builder_empty_constant PRIVATE
        turing_py_sink_s
        turing_py_nanobind_utils
        nanobind-static
        turing_test_embedded_python_s)

    add_turing_test(test_net_py_embedded_empty_constant PyEmbeddedEmptyConstantTest.cpp)
    nanobind_set_visibility(test_net_py_embedded_empty_constant)
    target_sources(test_net_py_embedded_empty_constant PRIVATE
        ${CMAKE_SOURCE_DIR}/python/turingdb/_embedded/PyTuringEmbedded.cpp)
    target_include_directories(test_net_py_embedded_empty_constant PRIVATE
        ${CMAKE_SOURCE_DIR}/python/turingdb/_embedded)
    target_link_libraries(test_net_py_embedded_empty_constant PRIVATE
        turing_py_nanobind_utils
        nanobind-static
        turing_test_embedded_python_s)
else()
    message(STATUS "No embeddable libpython: the test_net_py_* client tests are not built")
endif()

add_turing_test(test_net_turing_client_empty_constant TuringClientEmptyConstantTest.cpp)
target_sources(test_net_turing_client_empty_constant PRIVATE ProtoServerTest.cpp)

if (TURINGDB_BUILD_WASM)
    add_turing_test(test_net_wasm_optional_column_decode WasmOptionalColumnDecodeTest.cpp)
    target_link_libraries(test_net_wasm_optional_column_decode PRIVATE turing_db_wasm_s)

    add_turing_test(test_net_wasm_empty_constant_decode WasmEmptyConstantDecodeTest.cpp)
    target_link_libraries(test_net_wasm_empty_constant_decode PRIVATE turing_db_wasm_s)
endif()
