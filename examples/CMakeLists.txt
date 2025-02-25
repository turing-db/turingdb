
set(examples_sources SimpleGraph.cpp)

add_library(turing_db_examples_s STATIC ${examples_sources})

target_include_directories(turing_db_examples_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_examples_s PRIVATE
    turing_db_storage_s
    turing_db_system_s
    turing_db_jobs_s
    turing_db_s)
