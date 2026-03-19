add_turing_test(test_net_http_parser HTTPParserTest.cpp)
target_link_libraries(test_net_http_parser PRIVATE
    turing_db_http_common_s
    turing_db_http_parser_s)
