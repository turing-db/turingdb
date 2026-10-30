# A customer's journey planner over a rail timetable, on the graph JourneyTest builds.
function(add_journey_test exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_sources(${exe_name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/JourneyTest.cpp)
    target_include_directories(${exe_name} PRIVATE ${IR_TEST_DIR})
endfunction()

add_journey_test(test_query_ir_journey_legs JourneyLegsTest.cpp)
add_journey_test(test_query_ir_journey_pareto JourneyParetoTest.cpp)
add_journey_test(test_query_ir_journey_rounds JourneyRoundsTest.cpp)
