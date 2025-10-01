add_library(turing_testenv_s STATIC TuringTestEnv.cpp)

target_link_libraries(turing_testenv_s
    PUBLIC turing_db_s
           turing_db_memory_s)

target_include_directories(turing_testenv_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
