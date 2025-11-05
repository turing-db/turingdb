set(SAMPLE_NAME signature-tree)
set(SOURCES main.cpp)
set(${SAMPLE_NAME}_EXCLUDE_FROM_CI 1 PARENT_SCOPE)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME} PRIVATE
    turing_common_s)
