#pragma once

#include <ostream>
#include <unordered_set>

#include "decl/DeclID.h"

namespace db::v2 {

class CypherAST;
class SinglePartQuery;
class Match;
class Limit;
class Skip;
class Return;
class Projection;
class Pattern;
class PatternElement;
class WhereClause;
class Expression;
class NodePattern;
class EdgePattern;
class MapLiteral;
class BinaryExpression;
class UnaryExpression;
class SymbolExpression;
class LiteralExpression;
class ParameterExpression;
class PathExpression;
class NodeLabelExpression;
class StringExpression;
class PropertyExpression;
class VarDecl;
class AnalysisData;

struct NodePatternData;
struct EdgePatternData;
struct PropertyExpressionData;
struct NodeLabelExpressionData;
struct LiteralExpressionData;

class CypherASTDumper {
public:
    explicit CypherASTDumper(const CypherAST& ast);
    ~CypherASTDumper() = default;

    CypherASTDumper(const CypherASTDumper&) = delete;
    CypherASTDumper& operator=(const CypherASTDumper&) = delete;
    CypherASTDumper(CypherASTDumper&&) = delete;
    CypherASTDumper& operator=(CypherASTDumper&&) = delete;

    void dump(std::ostream& output);

private:
    const CypherAST& _ast;
    std::unordered_set<DeclID> _dumpedVariables;

    void dump(std::ostream& out, const SinglePartQuery& query);
    void dump(std::ostream& out, const Match& match);
    void dump(std::ostream& out, const Limit& lim);
    void dump(std::ostream& out, const Skip& skip);
    void dump(std::ostream& out, const Return& ret);
    void dump(std::ostream& out, const Projection& projection);
    void dump(std::ostream& out, const Pattern& pattern);
    void dump(std::ostream& out, const PatternElement& elem);
    void dump(std::ostream& out, const WhereClause& where);
    void dump(std::ostream& out, const NodePattern& node);
    void dump(std::ostream& out, const EdgePattern& edge);
    void dump(std::ostream& out, const MapLiteral& map);
    void dump(std::ostream& out, const Expression& expr);
    void dump(std::ostream& out, const BinaryExpression& expr);
    void dump(std::ostream& out, const UnaryExpression& expr);
    void dump(std::ostream& out, const SymbolExpression& expr);
    void dump(std::ostream& out, const LiteralExpression& expr);
    void dump(std::ostream& out, const ParameterExpression& expr);
    void dump(std::ostream& out, const PathExpression& expr);
    void dump(std::ostream& out, const NodeLabelExpression& expr);
    void dump(std::ostream& out, const StringExpression& expr);
    void dump(std::ostream& out, const PropertyExpression& expr);
    void dump(std::ostream& out, const VarDecl& decl);
};

}

