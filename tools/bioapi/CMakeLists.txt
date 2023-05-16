
set(bioapi_sources BioAPI.cpp)

add_executable(bioapi ${bioapi_sources})
target_link_libraries(bioapi PRIVATE
    turing_api_server_s
    tools_common_s)

target_include_directories(bioapi PRIVATE ${BIO_SOURCE_DIR}/messages)

install(TARGETS bioapi DESTINATION bin)
