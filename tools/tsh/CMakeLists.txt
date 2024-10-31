
set(tsh_sources
        TSH.cpp
        TuringShell.cpp)

add_executable(tsh ${tsh_sources})

target_link_libraries(tsh PRIVATE
    turing_client_native_s
    common_s
    tools_common_s
    tabulate::tabulate
    termcolor
    linenoise_s)

install(TARGETS tsh DESTINATION bin)
