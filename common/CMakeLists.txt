
set(common_sources
        Command.cpp
        Buffer.cpp
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
        JobSystem.cpp
        Demonology.cpp
        Process.cpp
        Panic.cpp
        TuringException.cpp
        Report.cpp)

add_library(common_s STATIC ${common_sources})
target_include_directories(common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(common_s PRIVATE ${BUILD_INFO_INCLUDE_DIR})

target_link_libraries(common_s
    turing_log_s
    ${Boost_LIBRARIES})
