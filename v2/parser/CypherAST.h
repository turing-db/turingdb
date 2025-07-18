#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "statements/StatementContainer.h"

namespace db {

class PatternNode;
class PatternEdge;
class PatternPart;
class PatternEntity;
class Pattern;
class Expression;
class Query;
class Projection;
class MapLiteral;
class SinglePartQuery;
class Statement;
class Scope;
class Symbol;

class CypherAST {
public:
    CypherAST();
    ~CypherAST();

    CypherAST(const CypherAST&) = delete;
    CypherAST(CypherAST&&) = delete;
    CypherAST& operator=(const CypherAST&) = delete;
    CypherAST& operator=(CypherAST&&) = delete;

    void beginScope();
    void endScope();

    PatternNode* nodeFromExpression(Expression* e);
    Pattern* newPattern();
    PatternPart* newPatternPart();
    PatternEdge* newOutEdge();
    PatternEdge* newInEdge();
    Projection* newProjection();
    MapLiteral* newMapLiteral();
    SinglePartQuery* newSinglePartQuery();
    PatternNode* newNode(std::optional<Symbol>&& symbol,
                         std::optional<std::vector<std::string_view>>&& labels,
                         MapLiteral* properties);

    template <typename T, typename... Args>
        requires std::is_base_of_v<Statement, T>
    T* newStatement(Args&&... args) {
        auto st = T::create(std::forward<Args>(args)...);
        auto* ptr = st.get();
        _statements.emplace_back(std::move(st));
        _currentStatements->add(ptr);

        return ptr;
    }

    template <typename T, typename... Args>
        requires std::is_base_of_v<Expression, T>
    T* newExpression(Args&&... args) {
        auto expr = T::create(std::forward<Args>(args)...);
        auto* ptr = expr.get();
        _expressions.emplace_back(std::move(expr));

        return ptr;
    }

    Scope& currentScope() {
        return *_currentScope;
    }

    const std::vector<std::unique_ptr<Query>>& queries() const {
        return _queries;
    }

private:
    std::unique_ptr<Scope> _rootScope;

    std::vector<std::unique_ptr<Expression>> _expressions;
    std::vector<std::unique_ptr<Pattern>> _patterns;
    std::vector<std::unique_ptr<PatternPart>> _patternParts;
    std::vector<std::unique_ptr<PatternEntity>> _patternEntity;
    std::vector<std::unique_ptr<Statement>> _statements;
    std::vector<std::unique_ptr<Projection>> _projections;
    std::vector<std::unique_ptr<Query>> _queries;
    std::vector<std::unique_ptr<MapLiteral>> _mapLiterals;
    std::vector<std::unique_ptr<StatementContainer>> _statementContainers;

    Scope* _currentScope = nullptr;
    StatementContainer* _currentStatements = nullptr;

    StatementContainer* newStatementContainer();
};

}
