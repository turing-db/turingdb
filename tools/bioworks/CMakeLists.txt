
set(bioworks_sources BioWorks.cpp)

add_executable(bioworks ${bioworks_sources})
target_link_libraries(bioworks PRIVATE
    turing_workflow_server_s
    tools_common_s)

install(TARGETS bioworks DESTINATION bin)
