
set(biorun_sources 
            NotebookRunner.cpp
            BioRun.cpp)

add_executable(biorun ${biorun_sources})
target_link_libraries(biorun PRIVATE
    tools_common_s
    turing_log_s)

install(TARGETS biorun DESTINATION bin)
