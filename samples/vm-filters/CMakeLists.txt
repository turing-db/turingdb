set(SAMPLE_NAME vm-filters)
set(SOURCES     main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE turing_samples_vm_utilslib_s)

target_include_directories(${SAMPLE_NAME}
    PRIVATE ${CMAKE_CURRENT_LIST_DIR})

install(
    FILES       ${CMAKE_CURRENT_LIST_DIR}/program.turing
    DESTINATION samples/${SAMPLE_NAME}/)
