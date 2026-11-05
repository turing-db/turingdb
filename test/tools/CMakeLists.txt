add_turing_test(test_tools_shell_completion ShellCompletionTest.cpp)
target_sources(test_tools_shell_completion PRIVATE ${CMAKE_SOURCE_DIR}/tools/turingdb/ShellCompletion.cpp)
target_include_directories(test_tools_shell_completion PRIVATE ${CMAKE_SOURCE_DIR}/tools/turingdb)

add_turing_test(test_tools_line_noise_handle LineNoiseHandleTest.cpp)
target_sources(test_tools_line_noise_handle PRIVATE
    ${CMAKE_SOURCE_DIR}/tools/turingdb/LineNoiseHandle.cpp
    ${CMAKE_SOURCE_DIR}/tools/turingdb/ProtectedLineNoiseSink.cpp)
target_include_directories(test_tools_line_noise_handle PRIVATE ${CMAKE_SOURCE_DIR}/tools/turingdb)

set(TURING_SHELL_SOURCES
    ${CMAKE_SOURCE_DIR}/tools/turingdb/TuringShell.cpp
    ${CMAKE_SOURCE_DIR}/tools/turingdb/ShellTable.cpp
    ${CMAKE_SOURCE_DIR}/tools/turingdb/ShellCompletion.cpp
    ${CMAKE_SOURCE_DIR}/tools/turingdb/LineNoiseHandle.cpp
    ${CMAKE_SOURCE_DIR}/tools/turingdb/ProtectedLineNoiseSink.cpp)

function(add_shell_test name source)
    add_turing_test(${name} ${source})
    target_sources(${name} PRIVATE ${TURING_SHELL_SOURCES})
    target_include_directories(${name} PRIVATE ${CMAKE_SOURCE_DIR}/tools/turingdb)
    target_link_libraries(${name} PRIVATE termcolor)
endfunction()

add_shell_test(test_tools_turing_shell_stop TuringShellStopTest.cpp)
add_shell_test(test_tools_shell_parameter ShellParameterTest.cpp)
