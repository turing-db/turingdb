
set(logging_sources
    LogSetup.cpp
    LogUtils.cpp)

add_library(turing_log_s STATIC ${logging_sources})
target_include_directories(turing_log_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(turing_log_s PUBLIC spdlog)
