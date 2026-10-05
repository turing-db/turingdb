# LOAD EMBEDDING writes the Parquet files it reads, so it needs the Parquet writer, which
# the engine links privately.
add_turing_test(test_query_ir_load_embedding_v3 LoadEmbeddingV3Test.cpp)
target_link_libraries(test_query_ir_load_embedding_v3 PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s
        turing_db_io_parquet_s)
target_include_directories(test_query_ir_load_embedding_v3 PRIVATE ${IR_TEST_DIR})

# VECTOR SEARCH runs against the session's vector indexes, which the system manager
# holds, so the search tests drive the whole engine the way the system-command ones do.
add_turing_test(test_query_ir_vector_search VectorSearchTest.cpp)
# The traversal cases read their rows as text, whatever kinds the projected columns are.
target_sources(test_query_ir_vector_search PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_vector_search PRIVATE ${IR_TEST_DIR})

# The composition cases drive the search through a WITH, a CALL and an aggregate, so they
# need the procedure-driving engine the same way the search tests do.
add_turing_test(test_query_ir_vector_search_composition VectorSearchCompositionTest.cpp)
target_sources(test_query_ir_vector_search_composition PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search_composition PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_vector_search_composition PRIVATE ${IR_TEST_DIR})

# The metric / index-kind cases build one index per case, so they drive the whole engine
# the way the other search tests do.
add_turing_test(test_query_ir_vector_search_index_kind VectorSearchIndexKindTest.cpp)
target_sources(test_query_ir_vector_search_index_kind PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search_index_kind PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_vector_search_index_kind PRIVATE ${IR_TEST_DIR})

# The write cases run their statements inside a change and submit it, so they need the
# change management the system manager holds.
add_turing_test(test_query_ir_vector_search_write VectorSearchWriteTest.cpp)
target_sources(test_query_ir_vector_search_write PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search_write PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_vector_search_write PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_vector_search_rejection VectorSearchRejectionTest.cpp)
target_sources(test_query_ir_vector_search_rejection PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_vector_search_rejection PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_vector_search_rejection PRIVATE ${IR_TEST_DIR})

# The neighbourhood ranking tests score a node's neighbours against its own embedding end
# to end through QueryInterpreterV3, over a purpose-built graph of 3-dimensional vectors,
# and read the nl dump of a top-K ranking back as text.
add_turing_test(test_query_ir_embedding_neighbour_ranking EmbeddingNeighbourRankingTest.cpp)
target_sources(test_query_ir_embedding_neighbour_ranking PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_embedding_neighbour_ranking PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_embedding_neighbour_ranking PRIVATE ${IR_TEST_DIR})

# The similarity-guided traversal tests prune each hop of a walk over an embedding graph.
add_turing_test(test_query_ir_similarity_guided_traversal SimilarityGuidedTraversalTest.cpp)
target_sources(test_query_ir_similarity_guided_traversal PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_similarity_guided_traversal PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_similarity_guided_traversal PRIVATE ${IR_TEST_DIR})

add_turing_test(test_query_ir_load_vector_duplicate LoadVectorDuplicateTest.cpp)
target_sources(test_query_ir_load_vector_duplicate PRIVATE ${IR_TEST_DIR}/StringRowSink.cpp)
target_link_libraries(test_query_ir_load_vector_duplicate PRIVATE
        turing_db_s
        turing_testenv_s
        turing_db_examples_s
        turing_db_system_s
        turing_db_storage_s
        turing_db_ir_exec_s)
target_include_directories(test_query_ir_load_vector_duplicate PRIVATE ${IR_TEST_DIR})
