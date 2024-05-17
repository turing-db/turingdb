
set(turing_app_sources
        TuringApp.cpp)

add_executable(turing-app ${turing_app_sources})
target_link_libraries(turing-app PRIVATE
    turing_app_server_s
    tools_common_s 
    turing_log_s
    argparse)

install(TARGETS turing-app DESTINATION bin)
