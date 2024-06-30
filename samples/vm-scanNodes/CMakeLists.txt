set(SAMPLE_NAME vm-scanNodes)
set(SOURCES     main.cpp)

turing_sample(${SAMPLE_NAME} ${SOURCES})

target_link_libraries(${SAMPLE_NAME}
    PRIVATE common_s
            turing_db2_vm_s
            turing_db2_import_neo4j_s)

target_include_directories(${SAMPLE_NAME}
    PRIVATE ${CMAKE_CURRENT_LIST_DIR})

install(
    FILES       ${CMAKE_CURRENT_LIST_DIR}/program.turing
    DESTINATION samples/${SAMPLE_NAME}/)
