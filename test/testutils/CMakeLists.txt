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

function(add_turing_test exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name} PRIVATE turing_testutils_s)
    gtest_discover_tests(${exe_name}
          DISCOVERY_TIMEOUT 60
          DISCOVERY_MODE PRE_TEST
          PROPERTIES ENVIRONMENT
              "TURING_EXTENSIONS_DIR=${CMAKE_BINARY_DIR}/lib/turingdb/extensions")
endfunction()
