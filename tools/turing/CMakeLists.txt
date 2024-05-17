
set(turing_sources
        TuringTool.cpp
        TuringToolConfig.cpp
        TuringStartCommand.cpp
        TuringStopCommand.cpp
        Turing.cpp)

add_executable(turing ${turing_sources})
target_link_libraries(turing PRIVATE
    tools_common_s    
    turing_log_s
    argparse)

install(TARGETS turing DESTINATION bin)
