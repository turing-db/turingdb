set(SAMPLE_NAME commit-index-sim)

set(SOURCES
    main.cpp
    Simulator.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    argparse
)
