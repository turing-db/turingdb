
set(http_common_sources HTTP.cpp)

add_library(turing_http_common_s STATIC ${http_common_sources})

target_include_directories(turing_http_common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_http_common_s PRIVATE common_s)