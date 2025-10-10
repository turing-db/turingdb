#pragma once

#include "views/GraphView.h"

namespace db {
class GraphMetadata;
}

namespace db::v2 {

class CypherAST;
class ExprAnalyzer;
class DeclContext;
class VarDecl;
class Stmt;
class MatchStmt;
class Skip;
class Limit;
class Pattern;
class PatternElement;
class NodePattern;
class EdgePattern;
class Expr;
class BinaryExpr;
class UnaryExpr;
class SymbolExpr;
class LiteralExpr;
class PropertyExpr;
class StringExpr;
class NodeLabelExpr;
class PathExpr;

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
    void analyze(const Stmt* stmt);
    void analyze(const MatchStmt* matchSt);
    void analyze(const Skip* skipSt);
    void analyze(const Limit* limitSt);

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

    void throwError(std::string_view msg, const void* obj = 0) const;
};

}
