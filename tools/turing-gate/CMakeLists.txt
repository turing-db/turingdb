
set(gate_sources TuringGate.cpp)

add_executable(turing-gate ${gate_sources})

target_link_libraries(turing-gate PRIVATE turing_gateway_s cpphttplib)

install(TARGETS turing-gate DESTINATION bin)
