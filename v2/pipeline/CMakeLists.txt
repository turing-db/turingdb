
set(pipeline_sources
    PipelineV2.cpp
    Processor.cpp
    PipelineBuffer.cpp
    PipelinePort.cpp
    PipelineInterface.cpp
    PipelineBuilder.cpp
    PipelineExecutor.cpp)

set(processors_sources
    processors/ScanNodesProcessor.cpp
    processors/GetInEdgesProcessor.cpp
    processors/GetEdgesProcessor.cpp
    processors/GetOutEdgesProcessor.cpp
    processors/GetPropertiesProcessor.cpp
    processors/MaterializeProcessor.cpp
    processors/MaterializeData.cpp
    processors/LambdaProcessor.cpp
    processors/SkipProcessor.cpp
    processors/LimitProcessor.cpp
    processors/CountProcessor.cpp
    processors/ProjectionProcessor.cpp
    processors/LambdaSourceProcessor.cpp
    processors/LambdaTransformProcessor.cpp)

add_library(turing_db_v2_pipeline_s STATIC ${pipeline_sources} ${processors_sources})

target_include_directories(turing_db_v2_pipeline_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_v2_pipeline_s PUBLIC
    turing_db_memory_s)

target_link_libraries(turing_db_v2_pipeline_s PRIVATE
    turing_db_storage_s)
