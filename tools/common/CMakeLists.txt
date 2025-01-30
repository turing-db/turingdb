
set(common_sources
        BannerDisplay.cpp
        ToolCommand.cpp
        ToolCommandEngine.cpp
        ToolInit.cpp)

add_library(turing_db_tools_common_s STATIC ${common_sources})

target_link_libraries(turing_db_tools_common_s PUBLIC
    turing_db_log_s
    turing_db_common_s
    argparse)

target_include_directories(turing_db_tools_common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(turing_db_tools_common_s PRIVATE ${BUILD_INFO_INCLUDE_DIR})
