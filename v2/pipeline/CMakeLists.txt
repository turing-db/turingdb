
set(pipeline_sources
    PipelineV2.cpp
    Processor.cpp
    PipelineBuffer.cpp
    PipelinePort.cpp
    PipelineExecutor.cpp)

set(processors_sources
    processors/ScanNodesProcessor.cpp
    processors/GetOutEdgesProcessor.cpp
    processors/MaterializeProcessor.cpp
    processors/MaterializeData.cpp
    processors/LambdaProcessor.cpp)

add_library(turing_db_v2_pipeline_s STATIC ${pipeline_sources} ${processors_sources})

target_include_directories(turing_db_v2_pipeline_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_v2_pipeline_s PRIVATE
    turing_db_memory_s
    turing_db_storage_s)
