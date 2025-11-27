
set(pipeline_sources
    PipelineV2.cpp
    Processor.cpp
    PipelineBuffer.cpp
    PipelinePort.cpp
    PipelineBuilder.cpp
    PipelineExecutor.cpp
    interfaces/PipelineInterface.cpp)

set(processors_sources
    processors/DatabaseProcedureProcessor.cpp
    processors/ScanNodesProcessor.cpp
    processors/GetInEdgesProcessor.cpp
    processors/GetEdgesProcessor.cpp
    processors/GetOutEdgesProcessor.cpp
    processors/GetPropertiesProcessor.cpp
    processors/GetPropertiesWithNullProcessor.cpp
    processors/MaterializeProcessor.cpp
    processors/ForkProcessor.cpp
    processors/MaterializeData.cpp
    processors/LambdaProcessor.cpp
    processors/SkipProcessor.cpp
    processors/LimitProcessor.cpp
    processors/CountProcessor.cpp
    processors/ProjectionProcessor.cpp
    processors/LambdaSourceProcessor.cpp
    processors/LambdaTransformProcessor.cpp
    processors/CartesianProductProcessor.cpp
    processors/HashJoinProcessor.cpp
    processors/LambdaSourceProcessor.cpp)

set(procedures_sources
    procedures/ProcedureBlueprint.cpp
    procedures/ProcedureBlueprintMap.cpp
    procedures/LabelsProcedure.cpp
    procedures/EdgeTypesProcedure.cpp
    procedures/PropertyTypesProcedure.cpp
    procedures/HistoryProcedure.cpp
)

add_library(turing_db_v2_pipeline_s
    STATIC ${pipeline_sources}
           ${processors_sources}
           ${procedures_sources})

target_include_directories(turing_db_v2_pipeline_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_v2_pipeline_s PUBLIC
    turing_db_memory_s)

target_link_libraries(turing_db_v2_pipeline_s PRIVATE
    turing_db_storage_s)
