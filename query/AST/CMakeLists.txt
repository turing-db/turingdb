
set(AST_sources 
        ASTContext.cpp
        DeclContext.cpp 
        Expr.cpp 
        ExprConstraint.cpp 
        MatchTarget.cpp
        PathPattern.cpp 
        QueryCommand.cpp 
        ReturnField.cpp 
        TypeConstraint.cpp 
        VarDecl.cpp
        ReturnProjection.cpp)

add_library(turing_db_query_AST_s STATIC ${AST_sources})

target_include_directories(turing_db_query_AST_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(turing_db_query_AST_s PRIVATE turing_db_storage_s)
