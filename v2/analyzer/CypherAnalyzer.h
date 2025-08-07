#pragma once

#include "decl/EvaluatedType.h"
#include "views/GraphView.h"

namespace db {

class CypherAST;
class SinglePartQuery;
class Match;
class Skip;
class Limit;
class Return;
class Pattern;
class PatternElement;
class NodePattern;
class EdgePattern;
class Expression;
class BinaryExpression;
class UnaryExpression;
class SymbolExpression;
class LiteralExpression;
class ParameterExpression;
class PropertyExpression;
class StringExpression;
class NodeLabelExpression;
class PathExpression;
class DeclContext;
class VarDecl;
class AnalysisData;

class CypherAnalyzer {
public:
    CypherAnalyzer(CypherAST& ast, GraphView graphView);
    ~CypherAnalyzer();

    CypherAnalyzer(const CypherAnalyzer&) = delete;
    CypherAnalyzer(CypherAnalyzer&&) = delete;
    CypherAnalyzer& operator=(const CypherAnalyzer&) = delete;
    CypherAnalyzer& operator=(CypherAnalyzer&&) = delete;

    const CypherAST& getAST() const { return _ast; }

    void analyze();

    // Query types
    void analyze(const SinglePartQuery& query);

    // Statemens
    void analyze(const Match& matchSt);
    void analyze(const Skip& skipSt);
    void analyze(const Limit& limitSt);
    void analyze(const Return& returnSt);

    // Pattern
    void analyze(const Pattern& pattern);
    void analyze(const PatternElement& element);
    void analyze(NodePattern& node);
    void analyze(EdgePattern& edge);

    // Expressions
    void analyze(Expression& expr);
    void analyze(BinaryExpression& expr);
    void analyze(UnaryExpression& expr);
    void analyze(SymbolExpression& expr);
    void analyze(LiteralExpression& expr);
    void analyze(ParameterExpression& expr);
    void analyze(PropertyExpression& expr);
    void analyze(StringExpression& expr);
    void analyze(NodeLabelExpression& expr);
    void analyze(PathExpression& expr);

private:
    CypherAST& _ast;
    GraphView _graphView;
    const GraphMetadata& _graphMetadata;
    DeclContext* _rootCtxt {nullptr};
    DeclContext* _ctxt {nullptr};

    void throwError(std::string_view msg, const void* obj = 0) const;

    bool propTypeCompatible(ValueType vt, EvaluatedType exprType) const;
};

}
