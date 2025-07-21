#pragma once

#include <iterator>
#include <ostream>
#include <vector>

namespace db {

class CypherAST;
class SinglePartQuery;
class Match;
class Limit;
class Skip;
class Return;
class Pattern;
class PatternPart;
class WhereClause;
class Expression;
class PatternNode;
class PatternEdge;
class Symbol;
class MapLiteral;
class BinaryExpression;
class UnaryExpression;
class AtomExpression;
class PathExpression;
class NodeLabelExpression;
class StringExpression;
class PropertyExpression;

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

    void dumpQuery(const SinglePartQuery& query);
    void dumpMatch(const Match& match);
    void dumpLimit(const Limit& lim);
    void dumpSkip(const Skip& skip);
    void dumpReturn(const Return& ret);
    void dumpPattern(const Pattern& pattern);
    void dumpPatternPart(const PatternPart& part);
    void dumpWhere(const WhereClause& where);
    void dumpNode(const PatternNode& node);
    void dumpEdge(const PatternEdge& edge);
    void dumpSymbol(const Symbol& symbol);
    void dumpMapLiteral(const MapLiteral& map);
    void dumpTypes(const std::vector<std::string_view>& types);
    void dumpExpression(const Expression& expr);
    void dumpBinaryExpression(const BinaryExpression& expr);
    void dumpUnaryExpression(const UnaryExpression& expr);
    void dumpAtomExpression(const AtomExpression& expr);
    void dumpPathExpression(const PathExpression& expr);
    void dumpNodeLabelExpression(const NodeLabelExpression& expr);
    void dumpStringExpression(const StringExpression& expr);
    void dumpPropertyExpression(const PropertyExpression& expr);
};

}

