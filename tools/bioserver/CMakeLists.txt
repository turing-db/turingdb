
set(bioserver_sources BioServer.cpp)

add_executable(bioserver ${bioserver_sources})
target_link_libraries(bioserver PRIVATE
    turing_api_server_s
    tools_common_s)

target_include_directories(bioserver PRIVATE ${BIO_SOURCE_DIR}/messages)

install(TARGETS bioserver DESTINATION bin)
