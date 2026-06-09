add_turing_test(test_server_authgate AuthGateTest.cpp)
target_sources(test_server_authgate PRIVATE
    ${CMAKE_SOURCE_DIR}/server/AuthGate.cpp)
target_include_directories(test_server_authgate PRIVATE
    ${CMAKE_SOURCE_DIR}/server)
target_link_libraries(test_server_authgate PRIVATE
    turing_db_auth_s
    turing_db_http_parser_s
    turing_db_http_common_s)
