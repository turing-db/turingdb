#pragma once

#include <span>
#include <string_view>
#include <vector>

#include "views/GraphView.h"

namespace db {

class CypherAST;
class Expr;
class VarDecl;
class ReadStmtAnalyzer;
class WriteStmtAnalyzer;
class ExprAnalyzer;
class QueryCommand;
class SinglePartQuery;
class LoadGraphQuery;
class CreateGraphQuery;
class DeclContext;
class ChangeQuery;
class LoadGMLQuery;
class LoadParquetQuery;
class LoadJsonlQuery;
class ChangeQuery;
class S3ConnectQuery;
class S3TransferQuery;
class CreateVectorIndexQuery;
class LoadVectorQuery;
class LoadEmbeddingQuery;
class InstallExtensionQuery;
class OrderBy;
class Skip;
class Limit;
class ReturnStmt;
class Stmt;
class WithStmt;
class Projection;
class CreateNodePropertyIndexQuery;
class CreateEdgePropertyIndexQuery;
class DropIndexQuery;

class CypherAnalyzer {
public:
    CypherAnalyzer(CypherAST* ast, GraphView graphView);
    ~CypherAnalyzer();

    CypherAnalyzer(const CypherAnalyzer&) = delete;
    CypherAnalyzer(CypherAnalyzer&&) = delete;
    CypherAnalyzer& operator=(const CypherAnalyzer&) = delete;
    CypherAnalyzer& operator=(CypherAnalyzer&&) = delete;

    CypherAST* getAST() const { return _ast; }

    void analyze();

    // Query types
    void analyze(const SinglePartQuery* query);
    void analyze(const ReturnStmt* returnSt);
    void analyze(const WithStmt* withSt);
    void analyze(const LoadGraphQuery* loadGraph);
    void analyze(const CreateGraphQuery* createGraph);
    void analyze(LoadGMLQuery* loadGML);
    void analyze(LoadParquetQuery* loadParquet);
    void analyze(LoadJsonlQuery* loadJsonl);
    void analyze(const S3ConnectQuery* s3Connect);
    void analyze(S3TransferQuery* s3Transfer);
    void analyze(const CreateVectorIndexQuery* query);
    void analyze(const LoadVectorQuery* query);
    void analyze(const LoadEmbeddingQuery* query);
    void analyze(const InstallExtensionQuery* query);
    void analyze(const CreateNodePropertyIndexQuery* query);
    void analyze(const CreateEdgePropertyIndexQuery* query);

    // Sub-statements
    void analyze(OrderBy* orderBySt, const Projection* projection);
    void analyze(Skip* skipSt);
    void analyze(Limit* limitSt);

    void setV3();

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    const GraphMetadata& _graphMetadata;
    DeclContext* _ctxt {nullptr};

    std::unique_ptr<ExprAnalyzer> _exprAnalyzer;
    std::unique_ptr<ReadStmtAnalyzer> _readAnalyzer;
    std::unique_ptr<WriteStmtAnalyzer> _writeAnalyzer;

    bool _isV3 {false};

    void analyzeProjection(Projection* projection, const Stmt* clause);
    void openWithScope(Projection* projection);
    void analyzeWithAliases(const Projection* projection) const;
    void analyzeWithOrderBy(const Projection* projection) const;
    void throwOnUnpublishedKeyVariable(const Expr* keyExpr, const Projection* projection) const;
    void declareItemAlias(Expr* item, std::string_view alias);
    void analyzeDistinct(const Projection* projection, const Stmt* clause) const;
    void analyzeNestedAggregates(const Projection* projection) const;
    void analyzeAggregateArguments(const Expr* expr, const Projection* projection) const;
    bool readsAnAggregateItem(const Expr* expr, const Projection* projection) const;
    void analyzeAggregateOrderBy(const Projection* projection) const;
    bool isGroupWise(const Expr* expr, const Projection* projection) const;
    bool isGroupWise(std::span<const Expr* const> exprs, const Projection* projection) const;

    void analyzeShortestPathReturn(const SinglePartQuery* query) const;
    const VarDecl* findConsumedVariable(const Expr* expr,
                                        const VarDecl* distDecl,
                                        const VarDecl* pathDecl) const;

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
