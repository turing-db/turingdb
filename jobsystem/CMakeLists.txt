
set(jobsystem_sources JobSystem.cpp)

add_library(turing_jobsystem_s
    STATIC ${jobsystem_sources})

target_include_directories(turing_jobsystem_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_jobsystem_s
    PUBLIC turing_log_s)
