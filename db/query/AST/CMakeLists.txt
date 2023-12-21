
set(AST_sources 
        ASTContext.cpp
        DeclContext.cpp 
        Expr.cpp 
        ExprConstraint.cpp 
        FromTarget.cpp 
        PathPattern.cpp 
        QueryCommand.cpp 
        SelectField.cpp 
        TypeConstraint.cpp 
        VarDecl.cpp
        SelectProjection.cpp)

add_library(turing_db_query_AST_s STATIC ${AST_sources})

target_include_directories(turing_db_query_parser_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})