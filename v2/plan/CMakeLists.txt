
set(plan_sources
    QueryPlanner.cpp
    PlanTree.cpp)

add_library(turing_db_v2_plan_s STATIC ${plan_sources})

target_link_libraries(turing_db_v2_plan_s PRIVATE
    turing_db_base_s
    turing_db_query_AST_s
    turing_common_s
    range-v3)

