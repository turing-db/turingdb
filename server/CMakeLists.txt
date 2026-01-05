
set(server_sources
    DBServerProcessor.cpp
    TuringServer.cpp)

add_library(turing_db_server_s STATIC ${server_sources})
target_include_directories(turing_db_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_server_s PRIVATE
    turing_db_s
    turing_db_http_parser_s
    turing_db_http_server_s
    turing_db_memory_s
    nlohmann_json)
