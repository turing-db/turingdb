
set(turing_sources
        TuringToolComponent.cpp
        TuringServerComponent.cpp
        TuringTool.cpp
        Turing.cpp)

add_executable(turing ${turing_sources})
target_link_libraries(turing PRIVATE
    tools_common_s    
    spdlog
    argparse)

install(TARGETS turing DESTINATION bin)
