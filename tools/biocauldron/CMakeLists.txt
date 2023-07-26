
set(biocauldron_sources BioCauldron.cpp)

add_executable(biocauldron ${biocauldron_sources})
target_link_libraries(biocauldron PRIVATE
    turing_uinew_server_s
    tools_common_s)
target_include_directories(biocauldron PRIVATE ${BIO_SOURCE_DIR}/messages)

install(TARGETS biocauldron DESTINATION bin)
