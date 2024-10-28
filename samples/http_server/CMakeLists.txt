set(SAMPLE_NAME http_server)
set(SOURCES main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE turing_http_server_s
            tools_common_s
            spdlog::spdlog)

target_include_directories(${SAMPLE_NAME}
    PRIVATE ${CMAKE_CURRENT_LIST_DIR})
