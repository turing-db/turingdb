
set(common_sources
        Command.cpp
        StringBuffer.cpp
        BannerDisplay.cpp
        FileUtils.cpp
        BioAssert.cpp
        TimerStat.cpp
        PerfStat.cpp)

add_library(common_s STATIC ${common_sources})
target_include_directories(common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(common_s PRIVATE ${BUILD_INFO_INCLUDE_DIR})
target_include_directories(common_s PRIVATE ${BIO_SOURCE_DIR}/messages)

target_link_libraries(common_s
    turing_log_s
    ${Boost_LIBRARIES})
