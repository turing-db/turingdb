#pragma once

#include "decl/EvaluatedType.h"
#include "views/GraphView.h"

namespace db::v2 {

class CypherAST;
class SinglePartQuery;
class MatchStmt;
class Skip;
class Limit;
class ReturnStmt;
class Pattern;
class PatternElement;
class NodePattern;
class EdgePattern;
class Expr;
class BinaryExpr;
class UnaryExpr;
class SymbolExpr;
class LiteralExpr;
class ParameterExpr;
class PropertyExpr;
class StringExpr;
class NodeLabelExpr;
class PathExpr;
class DeclContext;
class VarDecl;
class AnalysisData;

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

    // Statements
    void analyze(const MatchStmt* matchSt);
    void analyze(const Skip* skipSt);
    void analyze(const Limit* limitSt);
    void analyze(const ReturnStmt* returnSt);

    // Pattern
    void analyze(const Pattern* pattern);
    void analyze(const PatternElement* element);
    void analyze(NodePattern* node);
    void analyze(EdgePattern* edge);

    // Expressions
    void analyze(Expr* expr);
    void analyze(BinaryExpr* expr);
    void analyze(UnaryExpr* expr);
    void analyze(SymbolExpr* expr);
    void analyze(LiteralExpr* expr);
    void analyze(PropertyExpr* expr);
    void analyze(StringExpr* expr);
    void analyze(NodeLabelExpr* expr);
    void analyze(PathExpr* expr);

private:
    CypherAST* _ast {nullptr};
    GraphView _graphView;
    const GraphMetadata& _graphMetadata;
    DeclContext* _rootCtxt {nullptr};
    DeclContext* _ctxt {nullptr};

    void throwError(std::string_view msg, const void* obj = 0) const;
    bool propTypeCompatible(ValueType vt, EvaluatedType exprType) const;
    VarDecl* getOrCreateNamedVariable(EvaluatedType type, std::string_view name);
};

}
