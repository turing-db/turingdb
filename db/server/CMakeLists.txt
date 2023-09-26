
set(dbserver_sources
        DBServerConfig.cpp
        DBServer.cpp)

add_library(turing_db_server_s STATIC
    ${dbserver_sources})

target_include_directories(turing_db_server_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_server_s PRIVATE
    turing_log_s
    turing_db_core_s
    turing_db_query_s
    turing_db_manager_s)

target_link_libraries(turing_db_server_s PUBLIC turing_rest_s)

target_include_directories(turing_db_server_s PRIVATE ${BIO_SOURCE_DIR}/messages)
