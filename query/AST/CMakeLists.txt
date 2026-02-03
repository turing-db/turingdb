set(ast_sources
    CypherAST.cpp
    CypherASTDumper.cpp
    CypherError.cpp
    SourceManager.cpp
    DiagnosticsManager.cpp
    FunctionDecls.cpp

    decl/DeclContext.cpp
    decl/PatternData.cpp
    decl/VarDecl.cpp

    expr/ExprChain.cpp
    expr/BinaryExpr.cpp
    expr/LiteralExpr.cpp
    expr/EntityTypeExpr.cpp
    expr/PathExpr.cpp
    expr/PropertyExpr.cpp
    expr/StringExpr.cpp
    expr/SymbolExpr.cpp
    expr/UnaryExpr.cpp
    expr/FunctionInvocationExpr.cpp

    stmt/Stmt.cpp
    stmt/StmtContainer.cpp
    stmt/MatchStmt.cpp
    stmt/SetStmt.cpp
    stmt/SetItem.cpp
    stmt/CallStmt.cpp
    stmt/CreateStmt.cpp
    stmt/DeleteStmt.cpp
    stmt/ReturnStmt.cpp
    stmt/ShortestPathStmt.cpp
    stmt/OrderByItem.cpp
    stmt/OrderBy.cpp
    stmt/Limit.cpp
    stmt/Skip.cpp

    Pattern.cpp
    PatternElement.cpp
    EntityPattern.cpp
    Projection.cpp
    QueryCommand.cpp
    SinglePartQuery.cpp
    LoadGraphQuery.cpp
    ChangeQuery.cpp
    CommitQuery.cpp
    ListGraphQuery.cpp
    CreateGraphQuery.cpp
    LoadGMLQuery.cpp
    LoadNeo4jQuery.cpp
    LoadJsonlQuery.cpp
    S3ConnectQuery.cpp
    S3TransferQuery.cpp
    ShowProceduresQuery.cpp
    NodePattern.cpp
    EdgePattern.cpp
    Symbol.cpp
    SymbolChain.cpp
    Literal.cpp
    QualifiedName.cpp
    FunctionInvocation.cpp
    WhereClause.cpp
    YieldClause.cpp
    YieldItems.cpp)

add_library(turing_db_ast_s STATIC ${ast_sources})

target_include_directories(turing_db_ast_s
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_ast_s
  PRIVATE turing_db_query_common_s
          turing_db_storage_s
          turing_common_s
          range-v3
  PUBLIC turing_db_pipeline_s)

