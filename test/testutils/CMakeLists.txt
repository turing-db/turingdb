add_library(turing_testutils_s
    STATIC  TuringTestMain.cpp
            RemoteTestUtils.cpp)

target_link_libraries(turing_testutils_s
    PUBLIC  GTest::gtest
            GTest::gmock
            turing_common_s
            spdlog)

target_include_directories(turing_testutils_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})

# One ctest test per binary, not per case. gtest_discover_tests would report each
# case separately, at the cost of relaunching the binary once per case: every launch
# stands up a fresh SystemManager whose JobSystem costs CPU linear in core count.
function(add_turing_test exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE turing_testutils_s)

    add_test(NAME ${exe_name} COMMAND ${exe_name})
    set_tests_properties(${exe_name} PROPERTIES
        ENVIRONMENT "TURING_EXTENSIONS_DIR=${CMAKE_BINARY_DIR}/lib/turingdb/extensions")
endfunction()
