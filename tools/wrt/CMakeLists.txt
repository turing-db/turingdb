
set(wrt_sources
        RegressJob.cpp
        RegressTesting.cpp
        wrt.cpp)

add_executable(wrt ${wrt_sources})

target_link_libraries(wrt PRIVATE
    common_s
    tools_common_s
    turing_log_s)

install(TARGETS wrt DESTINATION bin)
