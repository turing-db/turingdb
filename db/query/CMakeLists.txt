flex_target(lexer ${CMAKE_CURRENT_SOURCE_DIR}/lexer.l
            ${CMAKE_CURRENT_BINARY_DIR}/lexer.cpp
            DEFINES_FILE ${CMAKE_CURRENT_BINARY_DIR}/lexer.h)

bison_target(parser ${CMAKE_CURRENT_SOURCE_DIR}/parser.y
             ${CMAKE_CURRENT_BINARY_DIR}/parser.cpp
             COMPILE_FLAGS "-d -v")

add_flex_bison_dependency(lexer parser)

set(plan_sources
    plan/PullPlan.cpp
    plan/LogicalOperator.cpp
    plan/Planner.cpp)

set(query_sources
        ${BISON_parser_OUTPUTS}
        ${FLEX_lexer_OUTPUTS}
        ${plan_sources}
        ASTContext.cpp
        Frame.cpp
        QueryCommand.cpp
        SelectField.cpp
        FromTarget.cpp
        PathPattern.cpp
        TypeConstraint.cpp
        ExprConstraint.cpp
        Expr.cpp
        QueryParser.cpp
        Symbol.cpp
        SymbolTable.cpp
        ExecutionContext.cpp
        Interpreter.cpp)

add_library(turing_db_query_s STATIC ${query_sources})
target_include_directories(turing_db_query_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(turing_db_query_s PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(turing_db_query_s PRIVATE
    turing_db_core_s
    turing_db_manager_s
    common_s)
