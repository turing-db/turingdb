set(plan_sources
        Planner.cpp
        QueryPlan.cpp
        ListDBStep.cpp
        OpenDBStep.cpp
        QueryPipeline.cpp
        QueryPipelineBuilder.cpp)

add_library(turing_db_query_plan_s STATIC ${plan_sources})
target_include_directories(turing_db_query_plan_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(turing_db_query_plan_s PRIVATE 
    turing_db_query_parser_s)
