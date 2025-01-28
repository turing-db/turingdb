
set(log_sources 
    LogSetup.cpp
    LogUtils.cpp)

add_library(turing_db_log_s STATIC ${log_sources})
target_include_directories(turing_db_log_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_log_s PUBLIC
    spdlog)
