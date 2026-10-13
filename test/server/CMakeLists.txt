add_turing_test(test_server_authgate AuthGateTest.cpp)
target_sources(test_server_authgate PRIVATE
    ${CMAKE_SOURCE_DIR}/server/AuthGate.cpp)
target_include_directories(test_server_authgate PRIVATE
    ${CMAKE_SOURCE_DIR}/server)
