set(interpreter_sources
    QueryInterpreterV2.cpp)

add_library(turing_db_v2_interpreter_s STATIC ${interpreter_sources})
target_include_directories(turing_db_v2_interpreter_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_v2_interpreter_s PUBLIC
    turing_common_s)

target_link_libraries(turing_db_v2_interpreter_s PRIVATE
    turing_db_system_s
    turing_db_jobs_s)
