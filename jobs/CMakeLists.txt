
set(jobs_sources
        JobSystem.cpp)

add_library(turing_db_jobs_s STATIC ${jobs_sources})
target_include_directories(turing_db_jobs_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_jobs_s PRIVATE
    turing_common_s
    spdlog)
