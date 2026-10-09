add_turing_test(test_net_http_parser HTTPParserTest.cpp)
target_link_libraries(test_net_http_parser PRIVATE
    turing_db_http_common_s
    turing_db_http_parser_s)

add_turing_test(test_net_turing_proto_framing TuringProtoFramingTest.cpp)
target_link_libraries(test_net_turing_proto_framing PRIVATE
    turing_db_proto_common_s)

add_turing_test(test_net_turing_proto_roundtrip TuringProtoRoundTripTest.cpp)
target_link_libraries(test_net_turing_proto_roundtrip PRIVATE
    turing_db_net_encoders
    turing_db_proto_client_s)

add_turing_test(test_net_json_encoder JsonEncoderTest.cpp)
target_link_libraries(test_net_json_encoder PRIVATE
    turing_db_net_encoders
    turing_db_memory_s)

add_turing_test(test_net_proto_analyze_error ProtoAnalyzeErrorTest.cpp)
target_link_libraries(test_net_proto_analyze_error PRIVATE
    turing_db_turing_proto_server_s
    turing_db_http_parser_s
    turing_db_http_common_s
    turing_db_proto_common_s)

add_turing_test(test_net_turing_client TuringClientTest.cpp)
target_link_libraries(test_net_turing_client PRIVATE
    turing_db_proto_client_s)

add_turing_test(test_net_turing_proto_endtoend TuringProtoEndToEndTest.cpp)
target_sources(test_net_turing_proto_endtoend PRIVATE
    ${CMAKE_SOURCE_DIR}/test/query-test-suite/QueryResultFormatter.cpp)

target_include_directories(test_net_turing_proto_endtoend PRIVATE
    ${CMAKE_SOURCE_DIR}/test/query-test-suite)
target_link_libraries(test_net_turing_proto_endtoend PRIVATE
    turing_db_server_s
    turing_db_proto_client_s
    turing_testenv_s)

add_turing_test(test_net_turing_proto_empty_result TuringProtoEmptyResultTest.cpp)
target_link_libraries(test_net_turing_proto_empty_result PRIVATE
    turing_db_server_s
    turing_db_proto_client_s
    turing_testenv_s
    turing_db_examples_s)

add_turing_test(test_net_json_encoder_double JsonEncoderDoubleTest.cpp)
target_link_libraries(test_net_json_encoder_double PRIVATE
    turing_db_net_encoders
    turing_db_memory_s)

add_turing_test(test_net_request_too_large RequestTooLargeTest.cpp)
target_link_libraries(test_net_request_too_large PRIVATE
    turing_db_server_s
    turing_db_proto_client_s
    turing_testenv_s
    turing_db_examples_s
    nlohmann_json)
