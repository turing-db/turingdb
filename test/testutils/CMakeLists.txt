add_library(turing_test_utils_s
    INTERFACE TuringTest.h)

target_link_libraries(turing_test_utils_s
    INTERFACE gtest_main
              common_s)

target_include_directories(turing_test_utils_s
    INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

function(add_turing_test exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE turing_test_utils_s)
    gtest_discover_tests(${exe_name})
endfunction()
