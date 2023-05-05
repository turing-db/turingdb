
add_executable(db_dumper_tests DBDumperTest.cpp)
target_link_libraries(db_dumper_tests PRIVATE
        turing_db_dumper_s
        turing_log_s
        gtest_main)

gtest_discover_tests(db_dumper_tests)
