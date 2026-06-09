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
