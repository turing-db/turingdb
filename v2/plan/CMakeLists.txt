
set(plan_sources
    PlanGraphGenerator.cpp
    PlanGraphVariables.cpp
    PlanGraph.cpp
    PlanGraphDebug.cpp
    PlanGraphTopology.cpp
    ReadStmtGenerator.cpp
    WriteStmtGenerator.cpp
    PipelineGenerator.cpp)

add_library(turing_db_v2_plan_s STATIC ${plan_sources})

target_include_directories(turing_db_v2_plan_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_v2_plan_s PRIVATE
    turing_db_v2_pipeline_s
    turing_db_base_s
    turing_db_storage_s
    turing_db_v2_ast_s
    turing_common_s
    range-v3)

