set(procedures_sources
    Procedure.cpp
    ProcedureData.cpp
    ProcedureException.cpp
    ProcedureManager.cpp
    ProcedureNamespace.cpp
    ProcedureTypeVector.cpp
    LabelsProcedure.cpp
    EdgeTypesProcedure.cpp
    PropertyTypesProcedure.cpp
    HistoryProcedure.cpp
    DescribeCommitProcedure.cpp
    ProceduresProcedure.cpp
    ShowIndexesProcedure.cpp
    HierarchicalLabelCountsProcedure.cpp
    ListNodesProcedure.cpp
    GetEdgesProcedure.cpp
    GetNodesProcedure.cpp
    GetNodeEdgesProcedure.cpp
    GnnNeighbourhoodSampleProcedure.cpp
    ProcUtils.cpp)

add_library(turing_db_procedures_s STATIC ${procedures_sources})

target_include_directories(turing_db_procedures_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_procedures_s
    PUBLIC turing_db_storage_s
           turing_common_s)
