
set(http_common_sources
    NetBuffer.cpp
    HTTP.cpp)

add_library(turing_db_http_common_s STATIC ${http_common_sources})

target_include_directories(turing_db_http_common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_http_common_s PRIVATE turing_common_s)
