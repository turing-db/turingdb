set(pipeline_sources
        operations/CountStep.cpp
        operations/TransformStep.cpp
        operations/GetOutEdgesStep.cpp
        operations/ScanEdgesStep.cpp
        operations/ScanNodesByLabelStep.cpp
        operations/ScanNodesStep.cpp
        operations/FilterStep.cpp
        operations/ScanOutEdgesByLabelStep.cpp
        operations/ScanInEdgesByLabelStep.cpp
        operations/CreateGraphStep.cpp
        operations/ListGraphStep.cpp
        operations/GetLabelSetIDStep.cpp
        operations/LoadGraphStep.cpp
        operations/LambdaStep.cpp
        
        PipelineStep.cpp
        Pipeline.cpp
        PipelineException.cpp
        Executor.cpp)

add_library(turing_db_pipeline_s STATIC ${pipeline_sources})

target_include_directories(turing_db_pipeline_s PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/operations)

target_link_libraries(turing_db_pipeline_s PRIVATE
    turing_db_query_AST_s
    turing_db_memory_s
    turing_db_storage_s
    turing_db_system_s
    turing_common_s)
