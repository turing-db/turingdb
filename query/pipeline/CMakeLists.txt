
set(pipeline_sources
    PipelineV2.cpp
    Processor.cpp
    PipelineBuffer.cpp
    PipelinePort.cpp
    PipelineBuilder.cpp
    PipelineExecutor.cpp
    PipelineException.cpp
    interfaces/PipelineInterface.cpp)

set(processors_sources
    processors/CallProcedureProcessor.cpp
    processors/ChangeProcessor.cpp
    processors/CommitProcessor.cpp
    processors/LoadCommitProcessor.cpp
    processors/ScanNodesProcessor.cpp
    processors/ScanNodesByLabelProcessor.cpp
    processors/ConstScanProcessor.cpp
    processors/ConstWriteSourceProcessor.cpp
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
    processors/ShortestPathProcessor.cpp
    processors/PathExplorerProcessor.cpp
    processors/HashJoinProcessor.cpp
    processors/WriteProcessor.cpp
    processors/GetLabelSetIDProcessor.cpp
    processors/GetEdgeTypeIDProcessor.cpp
    processors/LoadGraphProcessor.cpp
    processors/ListGraphProcessor.cpp
    processors/CreateGraphProcessor.cpp
    processors/LoadGMLProcessor.cpp
    processors/LoadJsonlProcessor.cpp
    processors/S3ConnectProcessor.cpp
    processors/S3PullProcessor.cpp
    processors/S3PushProcessor.cpp
    processors/ShowProceduresProcessor.cpp
    processors/LambdaSourceProcessor.cpp
    processors/ExprEvalProcessor.cpp
    processors/ExprProgram.cpp
    processors/PredicateProgram.cpp
    processors/FilterProcessor.cpp
    processors/LambdaSourceProcessor.cpp
    processors/ExprProgram.cpp
    processors/EvalBinaryExpr.cpp
    processors/EvalUnaryExpr.cpp
    processors/CSVSourceProcessor.cpp
    processors/CreateVectorIndexProcessor.cpp
    processors/LoadVectorProcessor.cpp
    processors/VectorSearchProcessor.cpp
    processors/DeleteVectorIndexProcessor.cpp
    processors/ShowVectorIndexesProcessor.cpp
    processors/OrderByProcessor.cpp
    processors/InstallExtensionProcessor.cpp
    processors/ShowExtensionsProcessor.cpp
    processors/EvalFunction.cpp
    processors/CreatePropertyIndexProcessor.cpp
    processors/IndexLookupProcessor.cpp
)

set(procedures_sources
    procedures/ProcedureData.cpp
    procedures/ProcedureTypeVector.cpp
    procedures/Procedure.cpp
    procedures/ProcedureNamespace.cpp
    procedures/ProcedureManager.cpp
    procedures/LabelsProcedure.cpp
    procedures/EdgeTypesProcedure.cpp
    procedures/PropertyTypesProcedure.cpp
    procedures/HistoryProcedure.cpp
    procedures/DescribeCommitProcedure.cpp
    procedures/ProceduresProcedure.cpp
    procedures/ShowIndexesProcedure.cpp
)

add_library(turing_db_pipeline_s
    STATIC ${pipeline_sources}
           ${processors_sources}
           ${procedures_sources})

target_include_directories(turing_db_pipeline_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_pipeline_s PUBLIC
    turing_db_memory_s)

target_link_libraries(turing_db_pipeline_s PRIVATE
    turing_extensions_s
    turing_db_storage_s
    turing_db_system_s
    turing_db_io_csv_s
    turing_vector_s)
