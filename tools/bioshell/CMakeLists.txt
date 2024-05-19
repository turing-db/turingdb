
set(bioshell_sources BioShell.cpp)

add_executable(bioshell ${bioshell_sources})

target_link_libraries(bioshell PRIVATE
    turing_client_native_s
    common_s
    tools_common_s
    linenoise_s)

install(TARGETS bioshell DESTINATION bin)
