set(SAMPLE_NAME pipe_getOutEdges)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE turing_samples_pipe_utilslib_s)

target_include_directories(${SAMPLE_NAME}
    PRIVATE ${CMAKE_CURRENT_LIST_DIR})