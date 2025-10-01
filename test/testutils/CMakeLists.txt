add_library(turing_testutils_s
    STATIC  TuringTestMain.cpp)

target_link_libraries(turing_testutils_s
    PUBLIC  GTest::gtest
            GTest::gmock
            turing_db_log_s
            turing_common_s
            spdlog)

target_include_directories(turing_testutils_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

function(add_turing_test exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE turing_testutils_s)
    gtest_discover_tests(${exe_name}
          DISCOVERY_TIMEOUT 10
          DISCOVER_MODE PRE_TEST)
endfunction()
