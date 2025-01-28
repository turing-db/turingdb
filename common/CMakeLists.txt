
set(common_sources 
        BioAssert.cpp
        RWSpinLock.cpp
        Panic.cpp
        TuringDBException.cpp
        TimerStat.cpp
        PerfStat.cpp)

if (${DEBUG_BUILD})
    target_compile_definitions(turing_db_common_s PUBLIC TURING_ASSERT=1)
endif()

add_library(turing_db_common_s STATIC ${common_sources})
target_include_directories(turing_db_common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_common_s PUBLIC
    spdlog
    expected)
