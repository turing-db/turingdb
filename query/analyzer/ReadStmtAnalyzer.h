#pragma once

#include "views/GraphView.h"

namespace db {

class CypherAST;
class ExprAnalyzer;
class DeclContext;
class VarDecl;
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
    void setV3() { _isV3 = true; }

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
    void analyze(const PatternElement* element);
    void analyze(NodePattern* node);
    void analyze(EdgePattern* edge);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    DeclContext* _ctxt {nullptr};
    ExprAnalyzer* _exprAnalyzer {nullptr};
    const GraphMetadata& _graphMetadata;
    bool _isV3 {false};

    void yieldEveryReturnValue(const FunctionSignature& signature, YieldClause* yield);

    // The predicate a YIELD ... WHERE filters the rows its statement produced with. Shared
    // by every statement that yields, since what a yield binds is what the predicate reads.
    void analyzeYieldFilter(const YieldItems* yieldItems);

    [[noreturn]] void throwError(std::string_view msg, const void* obj = 0) const;
};

}
