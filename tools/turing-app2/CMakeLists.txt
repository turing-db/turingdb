
set(turing_app2_sources
        TuringApp.cpp)

add_executable(turing-app2 ${turing_app2_sources})
target_link_libraries(turing-app2 PRIVATE
    turing_app2_backend_s
    tools_common_s
    turing_log_s
    argparse)

install(TARGETS turing-app2 DESTINATION bin)
