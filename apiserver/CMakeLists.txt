
set(apiserver_sources
        APIServerConfig.cpp
        APIServer.cpp)

add_library(turing_api_server_s STATIC ${apiserver_sources})

target_include_directories(turing_api_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_api_server_s PRIVATE
    crow
    turing_log_s)
