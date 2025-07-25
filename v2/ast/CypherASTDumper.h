#pragma once

#include "attribution/DeclID.h"
#include <iterator>
#include <ostream>
#include <unordered_set>

namespace db {

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
    std::ostream_iterator<char> _o;

    std::unordered_set<DeclID> _dumpedVariables;

    void dump(const SinglePartQuery& query);
    void dump(const Match& match);
    void dump(const Limit& lim);
    void dump(const Skip& skip);
    void dump(const Return& ret);
    void dump(const Projection& projection);
    void dump(const Pattern& pattern);
    void dump(const PatternElement& elem);
    void dump(const WhereClause& where);
    void dump(const NodePattern& node);
    void dump(const EdgePattern& edge);
    void dump(const MapLiteral& map);
    void dump(const Expression& expr);
    void dump(const BinaryExpression& expr);
    void dump(const UnaryExpression& expr);
    void dump(const SymbolExpression& expr);
    void dump(const LiteralExpression& expr);
    void dump(const ParameterExpression& expr);
    void dump(const PathExpression& expr);
    void dump(const NodeLabelExpression& expr);
    void dump(const StringExpression& expr);
    void dump(const PropertyExpression& expr);
    void dump(const VarDecl& decl);
};

}

