#pragma once

#include <vector>

namespace db::v2 {

class Expr;
class BinaryExpr;
class LiteralExpr;
class NodeLabelExpr;
class ParameterExpr;
class PathExpr;
class PropertyExpr;
class StringExpr;
class SymbolExpr;
class UnaryExpr;
class Pattern;
class PatternElement;
class EntityPattern;
class Projection;
class Stmt;
class CompoundStmt;
class MatchStmt;
class QueryCommand;
class SinglePartQuery;

class CypherAST {
public:
    friend BinaryExpr;
    friend LiteralExpr;
    friend NodeLabelExpr;
    friend ParameterExpr;
    friend PathExpr;
    friend PropertyExpr;
    friend StringExpr;
    friend SymbolExpr;
    friend UnaryExpr;
    friend Pattern;
    friend PatternElement;
    friend EntityPattern;
    friend Projection;
    friend CompoundStmt;
    friend MatchStmt;
    friend SinglePartQuery;

    CypherAST();
    ~CypherAST();

    CypherAST(const CypherAST&) = delete;
    CypherAST(CypherAST&&) = delete;
    CypherAST& operator=(const CypherAST&) = delete;
    CypherAST& operator=(CypherAST&&) = delete;

private:
    std::vector<Expr*> _expressions;
    std::vector<Pattern*> _patterns;
    std::vector<PatternElement*> _patternElems;
    std::vector<EntityPattern*> _entityPatterns;
    std::vector<Projection*> _projections;
    std::vector<Stmt*> _stmts;
    std::vector<QueryCommand*> _queries;

    void addExpr(Expr* expr);
    void addPattern(Pattern* pattern);
    void addPatternElement(PatternElement* element);
    void addEntityPattern(EntityPattern* pattern);
    void addProjection(Projection* projection);
    void addStmt(Stmt* stmt);
    void addQuery(QueryCommand* query);
};

}
