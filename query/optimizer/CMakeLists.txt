
set(optimizer_sources PlanOptimizer.cpp)

add_library(turing_db_optimizer_s STATIC ${optimizer_sources})
target_include_directories(turing_db_optimizer_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_optimizer_s PUBLIC
    turing_common_s)

target_link_libraries(turing_db_optimizer_s PRIVATE
    turing_db_plan_s
    turing_db_system_s)
