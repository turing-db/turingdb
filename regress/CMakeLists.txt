file(GLOB REGRESS_LIST ${CMAKE_CURRENT_SOURCE_DIR}/*/main.py)
file(REAL_PATH ${CMAKE_CURRENT_SOURCE_DIR}/pytester/ PYTESTER)

set(PYTURINGDB "turingdb @ git+https://github.com/turing-db/turingdb-sdk-python")

set(RUN_REGRESS_CONTENT "#!/bin/bash\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}set -e\n\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}[ $1 ] && PYTURINGDB=\"${PYTURINGDB}@$1\" || PYTURINGDB=turingdb\n")
set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}echo \"- Using turingdb: $PYTURINGDB\"\n\n")

foreach(test IN ITEMS ${REGRESS_LIST})
    get_filename_component(testdir ${test} DIRECTORY)
    set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}echo 'Running ${test}'\n")
    set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}cd ${testdir}\n")
    set(CMD "uv run \\\n")
    set(CMD "${CMD}    --with \"$PYTURINGDB\" \\\n")
    set(CMD "${CMD}    --project ${PYTESTER} \\\n")
    set(CMD "${CMD}    ${test}")
    set(RUN_REGRESS_CONTENT "${RUN_REGRESS_CONTENT}${CMD}\n")
endforeach()

file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/run_regress.sh
           ${RUN_REGRESS_CONTENT})

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/run_regress.sh
        DESTINATION regress)

add_custom_target(run_regress bash ${CMAKE_INSTALL_PREFIX}/regress/run_regress.sh
                  WORKING_DIRECTORY ${CMAKE_INSTALL_PREFIX}/regress)
