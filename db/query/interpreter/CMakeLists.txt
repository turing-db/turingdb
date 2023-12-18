
set(interpreter_sources
        QueryAnalyzer.cpp
        ExecutionContext.cpp 
        Interpreter.cpp)

add_library(turing_db_query_interpreter_s STATIC ${interpreter_sources})
target_include_directories(turing_db_query_interpreter_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(turing_db_query_interpreter_s PUBLIC 
    common_s)
target_link_libraries(turing_db_query_interpreter_s PRIVATE 
    turing_db_query_parser_s
    turing_db_query_plan_s)