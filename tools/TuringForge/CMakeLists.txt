
set(turing_forge_sources
    TuringForge.cpp
    HdrHistogram.cpp
    ForgeSpinners.cpp
    ForgeWorkload.cpp
    ForgeConnection.cpp
    ForgeErrorCounter.cpp
    ForgeWorkerThread.cpp)

add_executable(TuringForge ${turing_forge_sources})
target_link_libraries(TuringForge
    PUBLIC  turing_db_proto_common_s
            turing_db_proto_client_s
            argparse
            spdlog
    PRIVATE nlohmann_json)

install(TARGETS TuringForge DESTINATION bin)
