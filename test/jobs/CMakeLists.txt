add_turing_test(jobs_test JobSystemTest.cpp)

target_link_libraries(jobs_test
    PRIVATE turing_db_jobs_s)

target_include_directories(jobs_test
    PRIVATE ${CMAKE_CURRENT_LIST_DIR})

add_turing_test(jobs_queue_wait_test JobQueueWaitTest.cpp)

target_link_libraries(jobs_queue_wait_test
    PRIVATE turing_db_jobs_s)

target_include_directories(jobs_queue_wait_test
    PRIVATE ${CMAKE_CURRENT_LIST_DIR})
