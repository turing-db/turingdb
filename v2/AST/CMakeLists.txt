set(ast_sources
    CypherAST.cpp
    CypherASTDumper.cpp
    CypherError.cpp

    decl/DeclContext.cpp
    decl/PatternData.cpp
    decl/VarDecl.cpp

    expr/BinaryExpr.cpp
    expr/LiteralExpr.cpp
    expr/NodeLabelExpr.cpp
    expr/PathExpr.cpp
    expr/PropertyExpr.cpp
    expr/StringExpr.cpp
    expr/SymbolExpr.cpp
    expr/UnaryExpr.cpp
    expr/Literal.cpp

    stmt/Stmt.cpp
    stmt/StmtContainer.cpp
    stmt/MatchStmt.cpp
    stmt/ReturnStmt.cpp
    stmt/Limit.cpp
    stmt/Skip.cpp

    Pattern.cpp
    PatternElement.cpp
    EntityPattern.cpp
    Projection.cpp
    QueryCommand.cpp
    SinglePartQuery.cpp
    NodePattern.cpp
    EdgePattern.cpp
    Symbol.cpp
    QualifiedName.cpp
    WhereClause.cpp

    ASTException.cpp)

add_library(turing_db_v2_ast_s STATIC ${ast_sources})

target_include_directories(turing_db_v2_ast_s
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_v2_ast_s
  PRIVATE
    turing_common_s
    range-v3
  PUBLIC turing_db_storage_s)

