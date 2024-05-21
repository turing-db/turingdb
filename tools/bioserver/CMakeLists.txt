
set(bioserver_sources BioServer.cpp)

add_executable(bioserver ${bioserver_sources})
target_link_libraries(bioserver PRIVATE
    turing_db_server_s
    tools_common_s)

install(TARGETS bioserver DESTINATION bin)
