#pragma once

#include "views/GraphView.h"

namespace db {

class CypherAST;
class ExprAnalyzer;
class DeclContext;
class VarDecl;
class Symbol;
class Stmt;
class MatchStmt;
class YieldClause;
class CallStmt;
class VectorSearchStmt;
class OrderBy;
class Skip;
class Limit;
class Pattern;
class PatternElement;
class NodePattern;
class EdgePattern;
class FunctionInvocation;
class FunctionSignature;
class LoadCSVStmt;
class ShortestPathStmt;
class GraphMetadata;
class UnwindStmt;
class YieldItems;
class Expr;
class EdgePatternData;
class DeclContext;

class ReadStmtAnalyzer {
public:
    ReadStmtAnalyzer(CypherAST* ast, GraphView graphView);
    ~ReadStmtAnalyzer();

    ReadStmtAnalyzer(const ReadStmtAnalyzer&) = delete;
    ReadStmtAnalyzer(ReadStmtAnalyzer&&) = delete;
    ReadStmtAnalyzer& operator=(const ReadStmtAnalyzer&) = delete;
    ReadStmtAnalyzer& operator=(ReadStmtAnalyzer&&) = delete;

    void setDeclContext(DeclContext* ctxt) { _ctxt = ctxt; }
    void setExprAnalyzer(ExprAnalyzer* exprAnalyzer) { _exprAnalyzer = exprAnalyzer; }

    // Statements
    void analyze(Stmt* stmt);
    void analyze(const MatchStmt* matchSt);
    void analyze(const CallStmt* callSt);
    void analyze(LoadCSVStmt* loadCSVSt);
    void analyze(const VectorSearchStmt* vectorSearchSt);
    void analyze(const FunctionInvocation& func, const YieldClause* yieldSt);
    void analyze(OrderBy* orderBySt);
    void analyze(Skip* skipSt);
    void analyze(Limit* limitSt);
    void analyze(ShortestPathStmt* spSt);
    void analyze(UnwindStmt* unwind);

    // Pattern
    void analyze(const Pattern* pattern);
    void analyze(PatternElement* element);
    void analyze(NodePattern* node);
    void analyze(EdgePattern* edge);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    DeclContext* _ctxt {nullptr};
    ExprAnalyzer* _exprAnalyzer {nullptr};
    const GraphMetadata& _graphMetadata;

    void yieldEveryReturnValue(const FunctionSignature& signature, YieldClause* yield);

    // The property map of an edge pattern, each entry an equality predicate on @param decl:
    // a constraint on the matched edge, or - for a quantified pattern - one more predicate
    // every hop must pass
    void analyzeEdgeProperties(EdgePattern* edgePattern,
                               VarDecl* decl,
                               EdgePatternData* data,
                               bool asHopPredicates);

    // The hop of a quantified pattern: its edge, inner nodes and predicates resolve in a
    // scope of their own, where the names bind one entity; outside it the same names bind
    // the whole path's lists
    void analyzeHop(EdgePattern* edgePattern, EdgePatternData* data);

    // The name a quantified pattern's inner node groups, rejected when the query has
    // already bound it and declared as the list of that name otherwise
    void throwIfGroupNameIsBound(const DeclContext* outer, const Symbol* symbol, const EdgePattern* edgePattern) const;
    VarDecl* declareGroupVariable(DeclContext* outer, std::string_view name);

    // Declares the variable a `MATCH p = ...` names the whole element with, once its
    // entities are bound
    void analyzeNamedPath(PatternElement* element);

    void throwOnNamedPath(const Pattern* pattern);

    void enterScope(DeclContext* scope);

    // The predicate a YIELD ... WHERE filters the rows its statement produced with. Shared
    // by every statement that yields, since what a yield binds is what the predicate reads.
    void analyzeYieldFilter(const YieldItems* yieldItems);

    // Rejects a literal UNWIND argument that is no list: a literal carries its type at
    // plan time, so anything but a list - null aside, which unwinds into no row - is a
    // type error rather than a value to spread over rows
    void throwOnNonListLiteral(const Expr* arg) const;

    VarDecl* resolveShortestPathEndpoint(const Symbol* endpoint, const ShortestPathStmt* spSt) const;

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
