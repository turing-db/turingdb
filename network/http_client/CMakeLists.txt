
set(http_client_sources HTTPClient.cpp)

add_library(turing_http_client_s STATIC ${http_client_sources})

target_include_directories(turing_http_client_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_http_client_s PUBLIC
    turing_http_common_s
    common_s)