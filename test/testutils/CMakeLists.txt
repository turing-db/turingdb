add_library(turing_test_utils_s
    STATIC  TuringTestMain.cpp)

target_link_libraries(turing_test_utils_s
    PUBLIC  GTest::gtest
            common_s)

target_include_directories(turing_test_utils_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

function(add_turing_test exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE turing_test_utils_s)
    gtest_discover_tests(${exe_name}
          DISCOVERY_TIMEOUT 10
          DISCOVER_MODE PRE_TEST)
endfunction()
