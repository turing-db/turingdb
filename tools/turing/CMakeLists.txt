
set(turing_sources
        TuringTool.cpp
        TuringStartCommand.cpp
        TuringStopCommand.cpp
        TuringStart2Command.cpp
        Turing.cpp)

add_executable(turing ${turing_sources})
target_link_libraries(turing PRIVATE
    tools_common_s    
    turing_log_s
    argparse)

install(TARGETS turing DESTINATION bin)
