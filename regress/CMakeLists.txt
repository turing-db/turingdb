macro(regress_test REGRESS_NAME SOURCES)
    set( _SOURCES ${SOURCES} ${ARGN} ) 
    install(FILES ${_SOURCES}
        DESTINATION regress/${REGRESS_NAME})

    set(REGRESS_RUNSH ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

    # check that run.sh exists
    if (NOT EXISTS "${REGRESS_RUNSH}")
        message(FATAL_ERROR "Run script for the regress does not exist ${REGRESS_RUNSH}")
    endif()

    # add regress name to list
    set(REGRESS_RUNSH_INSTALL ${CMAKE_INSTALL_PREFIX}/regress/${REGRESS_NAME}/run.sh)
    list(APPEND REGRESS_LIST ${REGRESS_RUNSH_INSTALL})
    set(REGRESS_LIST ${REGRESS_LIST} PARENT_SCOPE)
endmacro()

# subdirectories
add_subdirectory(create_turingdb_object)
add_subdirectory(json_error_msg)
add_subdirectory(list_graphs)
add_subdirectory(change_submit_alice)
add_subdirectory(change_10)
add_subdirectory(submit_empty_change)
add_subdirectory(create_edges_inject)
add_subdirectory(call_metadata)
add_subdirectory(complex_rebases)
add_subdirectory(serialization)
add_subdirectory(snapshot_isolation)
add_subdirectory(multi_type_properties)
add_subdirectory(count_simpledb)
add_subdirectory(count_alias_simpledb)
add_subdirectory(read_script)
add_subdirectory(neo4j_import)
add_subdirectory(wine_ontology)
add_subdirectory(bulk_create_crash)
add_subdirectory(get_nodes_invalid_id)

# Python SDK version to be used by uv
# Can be overridden with -DPYTURINGDB=/path/to/wheel.whl
# By default, looks for a wheel in wheelhouse/ or dist/
if(NOT DEFINED PYTURINGDB)
    file(GLOB _WHEELHOUSE_WHEELS "${CMAKE_SOURCE_DIR}/wheelhouse/*.whl")
    file(GLOB _DIST_WHEELS "${CMAKE_SOURCE_DIR}/dist/*.whl")
    if(_WHEELHOUSE_WHEELS)
        list(GET _WHEELHOUSE_WHEELS 0 PYTURINGDB)
        message(STATUS "Using wheel from wheelhouse: ${PYTURINGDB}")
    elseif(_DIST_WHEELS)
        list(GET _DIST_WHEELS 0 PYTURINGDB)
        message(STATUS "Using wheel from dist: ${PYTURINGDB}")
    else()
        set(PYTURINGDB "")
        message(STATUS "No wheel found - run_regress will fail until wheel is built")
    endif()
endif()

# write run_regress target
set(RUN_REGRESS_CONTENT "#!/bin/bash\nset -e\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}export TURING_HOME=\"${CMAKE_INSTALL_PREFIX}\"\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}export PATH=\"\$TURING_HOME/bin:\$PATH\"\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}export PYTURINGDB=\"${PYTURINGDB}\"\n\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}if [[ -z \"\$PYTURINGDB\" || ! -f \"\$PYTURINGDB\" ]]; then\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}    echo \"Error: No wheel found. Build the wheel first with: uv build --wheel\" >&2\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}    echo \"Then re-run: cmake .. && make install && make run_regress\" >&2\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}    exit 1\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}fi\n\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}echo \"- Using turingdb: \$PYTURINGDB\"\n\n")
foreach(test IN ITEMS ${REGRESS_LIST})
    set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}bash ${test}\n")
endforeach()

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/run_regress.sh
           ${RUN_REGRESS_CONTENT})

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/run_regress.sh
        DESTINATION regress)

add_custom_target(run_regress bash ${CMAKE_INSTALL_PREFIX}/regress/run_regress.sh
                  WORKING_DIRECTORY ${CMAKE_INSTALL_PREFIX}/regress)
