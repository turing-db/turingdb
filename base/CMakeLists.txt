
add_library(turing_db_base_s STATIC QueryCallbacks.cpp)
target_include_directories(turing_db_base_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_base_s PUBLIC turing_db_log_s)
