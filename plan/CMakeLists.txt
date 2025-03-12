
set(plan_sources
    PlannerException.cpp
    QueryPlanner.cpp)

add_library(turing_db_plan_s STATIC ${plan_sources})
target_link_libraries(turing_db_plan_s PRIVATE
    turing_db_storage_s
    turing_db_query_AST_s
    turing_db_pipeline_s
    turing_db_memory_s
    turing_db_system_s
    turing_common_s)

target_link_libraries(turing_db_plan_s PUBLIC turing_db_base_s)

target_include_directories(turing_db_plan_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
