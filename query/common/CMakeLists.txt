
set(common_sources CompilerException.cpp)

add_library(turing_db_query_common_s STATIC ${common_sources})

target_include_directories(turing_db_query_common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_query_common_s
    PUBLIC turing_common_s)
