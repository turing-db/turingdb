
set(common_sources 
        BioAssert.cpp
        RWSpinLock.cpp
        Panic.cpp
        TuringException.cpp
        FatalException.cpp
        TimerStat.cpp
        PerfStat.cpp
        FileUtils.cpp
        Demonology.cpp
        BannerDisplay.cpp
        ToolInit.cpp
        ToolCommand.cpp
        ToolCommandEngine.cpp
        Process.cpp
        ProcessUtils.cpp
        Command.cpp
        StringUtils.cpp
        Profiler.cpp

        log/LogSetup.cpp
        log/LogUtils.cpp)

add_library(turing_common_s STATIC ${common_sources})
if (${DEBUG_BUILD})
    target_compile_definitions(turing_common_s PUBLIC TURING_ASSERT=1)
endif()

target_include_directories(turing_common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_compile_definitions(turing_common_s PUBLIC -DTURING_PROFILE=${TURING_PROFILE})

target_link_libraries(turing_common_s PUBLIC
    spdlog
    expected
    argparse)
