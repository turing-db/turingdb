#pragma once

#include <span>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "stmt/CallSubqueryStmt.h"
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
class UnionQuery;
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
class StmtContainer;
class WithStmt;
class ExistsExpr;
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
    void analyze(const UnionQuery* query);
    void analyze(const ReturnStmt* returnSt);
    void analyze(const WithStmt* withSt);
    void analyze(CallSubqueryStmt* subquery);
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

    // Analyzes the body of an EXISTS subquery under a scope of its own, seeded with the
    // variables in flight so the body reads them and binds nothing outside itself
    void analyzeExistsBody(ExistsExpr* exists);

    // Sub-statements
    void analyze(OrderBy* orderBySt, const Projection* projection);
    void analyze(Skip* skipSt);
    void analyze(Limit* limitSt);

private:
    using DeclSet = std::unordered_set<const VarDecl*>;

    CypherAST* _ast {nullptr};
    GraphView _graphView;
    const GraphMetadata& _graphMetadata;
    DeclContext* _ctxt {nullptr};

    std::unique_ptr<ExprAnalyzer> _exprAnalyzer;
    std::unique_ptr<ReadStmtAnalyzer> _readAnalyzer;
    std::unique_ptr<WriteStmtAnalyzer> _writeAnalyzer;

    // The names the scope clause of the subquery being analyzed imports, empty outside one
    // and while a nested body is analyzed under its own. A body importing through a
    // leading WITH holds none: those names are an ordinary projection, which an ordinary
    // WITH descopes.
    std::vector<std::string_view> _subqueryImports;

    // The shared body of a query and of a subquery's. @param returnRequired says whether a
    // body ending on a reading clause needs a RETURN: an EXISTS body does not
    void analyzeQueryBody(const SinglePartQuery* query, bool returnRequired);

    void analyzeProjection(Projection* projection, const Stmt* clause);
    void openWithScope(Projection* projection);

    // Declares one variable per projected item in @param scope, under the name the item
    // publishes, and records it on the projection for the code generator to bind
    void publishProjection(Projection* projection, DeclContext* scope);

    void setScope(DeclContext* scope);

    // A body opening on a WITH of plain variables imports them, when no scope clause says
    // what the body reads
    void importThroughLeadingWith(CallSubqueryStmt::Branch& branch) const;

    // Analyzes one query of a CALL body under its own scope, seeded with what it imports
    void analyzeSubqueryBranch(const CallSubqueryStmt::Branch& branch, bool hasScopeClause);

    // Analyzes one query of an EXISTS body, correlated with the variables in flight
    void analyzeExistsBranch(const SinglePartQuery* body);

    // Declares what a returning body publishes in the scope around the CALL, rejecting a
    // name that scope already holds
    void publishSubqueryReturn(const CallSubqueryStmt* subquery);
    void publishUnionBranchReturn(Projection* branchProjection, const Projection* firstProjection);

    // Adds to a barrier of a subquery body the imports it does not project, so what the
    // scope clause named stays readable below it. Answers whether it added one.
    bool carrySubqueryImports(Projection* projection, bool isAggregate, bool hasGroupingKeys) const;

    // Rejects the item that publishes an imported name under another variable
    void throwOnRedeclaredImport(const Projection* projection,
                                 std::string_view import,
                                 const VarDecl* decl) const;
    void analyzeWithAliases(const Projection* projection) const;
    void analyzeWithOrderBy(const Projection* projection) const;
    void throwOnUnpublishedKeyVariable(const Expr* keyExpr, const Projection* projection) const;
    void declareItemAlias(Expr* item, std::string_view alias);
    void analyzeDistinct(const Projection* projection, bool isAggregate) const;
    void analyzeNestedAggregates(const Projection* projection) const;
    void analyzeAggregateArguments(const Expr* expr, const Projection* projection) const;
    bool readsAnAggregateItem(const Expr* expr, const Projection* projection) const;
    void analyzeAggregateOrderBy(const Projection* projection) const;
    bool isGroupWise(const Expr* expr, const Projection* projection) const;
    bool isGroupWise(const Expr* expr, const Projection* projection, DeclSet& elements) const;
    bool isGroupWise(std::span<const Expr* const> exprs,
                     const Projection* projection,
                     DeclSet& elements) const;

    // Every branch of a union must project the same columns, in the same order and
    // under the same names: the union emits one result table, so a branch naming
    // other columns has no column of that table to fill
    void analyzeUnionColumns(std::span<const SinglePartQuery* const> branches) const;
    const Projection* unionBranchProjection(const SinglePartQuery* branch) const;
    static void collectProjectionNames(const Projection* projection,
                                       std::vector<std::string_view>& names);

    void throwOnReadAfterUpdate(const StmtContainer* stmts) const;
    void analyzeShortestPathReturn(const SinglePartQuery* query) const;
    const VarDecl* findConsumedVariable(const Expr* expr,
                                        const VarDecl* distDecl,
                                        const VarDecl* pathDecl) const;

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
