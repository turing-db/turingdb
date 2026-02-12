
regress_test(test_suite_queries run.sh main.py)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/test/query-test-suite/tests/
        DESTINATION regress/test_suite_queries/query-tests
        FILES_MATCHING PATTERN "*.json")
