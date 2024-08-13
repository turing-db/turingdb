macro(turing_sample SAMPLE_NAME SOURCES)
    list(APPEND SAMPLE_LIST ${SAMPLE_NAME})
    set(_SOURCES ${SOURCES} ${ARGN}) # Hack to parse SOURCES as a list

    set(SAMPLE_LIST ${SAMPLE_LIST} PARENT_SCOPE)
    add_executable(${SAMPLE_NAME} ${_SOURCES})

    file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/run.sh
               "#!/bin/bash\n"
               "./${SAMPLE_NAME}")
    file(CHMOD ${CMAKE_CURRENT_BINARY_DIR}/run.sh
         PERMISSIONS OWNER_EXECUTE GROUP_EXECUTE WORLD_EXECUTE OWNER_WRITE OWNER_READ)

    install(
        TARGETS     ${SAMPLE_NAME}
        DESTINATION samples/${SAMPLE_NAME})

    install(
        PROGRAMS    ${CMAKE_CURRENT_BINARY_DIR}/run.sh
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
        DESTINATION samples/${SAMPLE_NAME})
    target_compile_definitions(
        ${SAMPLE_NAME}
        PRIVATE SAMPLE_NAME="${SAMPLE_NAME}")
endmacro()

add_subdirectory(vm-utilslib)
add_subdirectory(import_pole)
#add_subdirectory(import_reactome)
add_subdirectory(scan_nodes_iterator)
add_subdirectory(bytecompiler)
#add_subdirectory(scan_nodes_processor)
add_subdirectory(vm-basic)
add_subdirectory(vm-scanEdges)
#add_subdirectory(vm-reactome)
add_subdirectory(vm-scanNodes)
add_subdirectory(vm-scanNodesByLabel)
add_subdirectory(vm-scanOutEdgesByLabel)
add_subdirectory(vm-scanInEdgesByLabel)
add_subdirectory(vm-getOutEdges)
add_subdirectory(vm-getInEdges)
add_subdirectory(vm-sparseOutput)
add_subdirectory(vm-complex)
add_subdirectory(vm-simpleGraph)
add_subdirectory(vm-scanNodeProperties)
add_subdirectory(vm-scanNodePropertiesByLabel)
add_subdirectory(vm-scanEdgeProperties)
add_subdirectory(vm-scanNodePropertiesFiltered)
add_subdirectory(vm-filters)

set (SCRIPT_LIST_CONTENT "")
list (LENGTH SAMPLE_LIST SAMPLE_COUNT)
message(STATUS "Building ${SAMPLE_COUNT} samples:")
foreach(sample IN ITEMS ${SAMPLE_LIST})
    message("   - ${sample}")
    set (SCRIPT_LIST_CONTENT "${sample}\n${SCRIPT_LIST_CONTENT}")
endforeach()

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/test.list
           ${SCRIPT_LIST_CONTENT})

install(
    FILES       ${CMAKE_CURRENT_BINARY_DIR}/test.list
    DESTINATION samples/${SAMPLE_NAME})
