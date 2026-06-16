
set(auth_sources
    Authenticator.cpp)

add_library(turing_db_auth_s STATIC ${auth_sources})

target_include_directories(turing_db_auth_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_auth_s
    PUBLIC turing_common_s)
