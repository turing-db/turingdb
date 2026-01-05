
set(db_sources
    TuringDB.cpp)

add_library(turing_db_s STATIC ${db_sources})
target_include_directories(turing_db_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_s PUBLIC
    turing_db_pipeline_s
    turing_db_interpreter_s
    turing_db_system_s
    turing_common_s)
