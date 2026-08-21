add_turing_test(test_procedures_registration ProcedureRegistrationTest.cpp)
target_link_libraries(test_procedures_registration PRIVATE turing_db_procedures_s)

# The gnn sample and describeCommit chunking tests drive real registered procedures
# against the shared SimpleGraph fixture.
add_turing_test(test_procedures_gnn_neighbourhood_sample_chunk GnnNeighbourhoodSampleChunkTest.cpp)
target_link_libraries(test_procedures_gnn_neighbourhood_sample_chunk PRIVATE
        turing_db_procedures_s
        turing_db_storage_s
        turing_db_jobs_s
        turing_db_examples_s)

add_turing_test(test_procedures_describe_commit_chunk DescribeCommitChunkTest.cpp)
target_link_libraries(test_procedures_describe_commit_chunk PRIVATE
        turing_db_procedures_s
        turing_db_storage_s
        turing_db_jobs_s
        turing_db_examples_s)
