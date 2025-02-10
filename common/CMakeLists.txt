
set(common_sources
        Command.cpp
        StringBuffer.cpp
        BannerDisplay.cpp
        FileUtils.cpp
        BioAssert.cpp
        TimerStat.cpp
        PerfStat.cpp
        Report.cpp
        SpinLock.cpp
        RWSpinLock.cpp
        ProcessUtils.cpp
        Buffer.cpp
        Demonology.cpp
        Process.cpp
        Panic.cpp
        TuringException.cpp
        Report.cpp)

add_library(common_s STATIC ${common_sources})
target_include_directories(common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(common_s PUBLIC ${BUILD_INFO_INCLUDE_DIR})
if (${DEBUG_BUILD})
    target_compile_definitions(common_s PUBLIC TURING_ASSERT=1)
endif()

target_link_libraries(common_s
    PUBLIC expected::expected
           turing_log_s
           turing_jobsystem_s
           ${Boost_LIBRARIES})
