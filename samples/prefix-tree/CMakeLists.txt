set(SAMPLE_NAME prefix-tree)
set(SOURCES
    main.cpp
    prefix-tree.cpp
    utils.cpp
    cli.cpp
    benchmark.cpp
)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_common_s)
