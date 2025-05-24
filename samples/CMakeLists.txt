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
    target_compile_definitions(
        ${SAMPLE_NAME}
        PRIVATE SAMPLE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
endmacro()

#add_subdirectory(aws-s3-list)
#add_subdirectory(aws-s3-read)
#add_subdirectory(aws-s3-stream)
#add_subdirectory(aws-s3-write)
#add_subdirectory(import_pole)
#add_subdirectory(import-gml)
#add_subdirectory(import_reactome)
#add_subdirectory(scan_nodes_iterator)
#add_subdirectory(pipe_utils_lib)
#add_subdirectory(pipe_scan_nodes)
#add_subdirectory(pipe_getOutEdges)
#add_subdirectory(pipe_scanNodesByLabel)
#add_subdirectory(simple_server)
#add_subdirectory(http_server)
add_subdirectory(fs-mapped)
add_subdirectory(fs-file)
add_subdirectory(fs-page-reader)
add_subdirectory(fs-page-writer)
add_subdirectory(fs-reader-writer)
add_subdirectory(graph-loader-dumper)
add_subdirectory(simpledb)
add_subdirectory(signature-tree)
add_subdirectory(s3testcli)
add_subdirectory(dist-vectors)

set (SCRIPT_LIST_CONTENT "")
list (LENGTH SAMPLE_LIST SAMPLE_COUNT)
message(STATUS "Building ${SAMPLE_COUNT} samples:")
foreach(sample IN ITEMS ${SAMPLE_LIST})
    # if not excluded from CI, add to script list
    if (${${sample}_EXCLUDE_FROM_CI})
        message("   - ${sample} (excluded from CI pipeline)")
    else()
        message("   - ${sample}")
        set (SCRIPT_LIST_CONTENT "${sample}\n${SCRIPT_LIST_CONTENT}")
    endif()
endforeach()

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/test.list
           ${SCRIPT_LIST_CONTENT})

install(
    FILES       ${CMAKE_CURRENT_BINARY_DIR}/test.list
                ${CMAKE_CURRENT_SOURCE_DIR}/run-samples.sh
    DESTINATION samples/${SAMPLE_NAME})
