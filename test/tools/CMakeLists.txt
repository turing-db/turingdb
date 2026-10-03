add_turing_test(test_tools_shell_completion ShellCompletionTest.cpp)
target_sources(test_tools_shell_completion PRIVATE ${CMAKE_SOURCE_DIR}/tools/turingdb/ShellCompletion.cpp)
target_include_directories(test_tools_shell_completion PRIVATE ${CMAKE_SOURCE_DIR}/tools/turingdb)
target_link_libraries(test_tools_shell_completion PRIVATE linenoise_s)

add_turing_test(test_tools_line_noise_handle LineNoiseHandleTest.cpp)
target_sources(test_tools_line_noise_handle PRIVATE
    ${CMAKE_SOURCE_DIR}/tools/turingdb/LineNoiseHandle.cpp
    ${CMAKE_SOURCE_DIR}/tools/turingdb/ProtectedLineNoiseSink.cpp)
target_include_directories(test_tools_line_noise_handle PRIVATE ${CMAKE_SOURCE_DIR}/tools/turingdb)
target_link_libraries(test_tools_line_noise_handle PRIVATE linenoise_s)
