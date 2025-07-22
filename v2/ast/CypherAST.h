#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "statements/StatementContainer.h"
#include "statements/SubStatement.h"

namespace db {

class NodePattern;
class EdgePattern;
class PatternElement;
class PatternEntity;
class Pattern;
class Expression;
class QueryCommand;
class Projection;
class MapLiteral;
class SinglePartQuery;
class Statement;
class Symbol;

class CypherAST {
public:
    CypherAST();
    ~CypherAST();

    CypherAST(const CypherAST&) = delete;
    CypherAST(CypherAST&&) = delete;
    CypherAST& operator=(const CypherAST&) = delete;
    CypherAST& operator=(CypherAST&&) = delete;

    NodePattern* nodeFromExpression(Expression* e);
    Pattern* newPattern();
    PatternElement* newPatternElem();
    EdgePattern* newOutEdge(const std::optional<Symbol>& symbol,
                            std::optional<std::vector<std::string_view>>&& types,
                            MapLiteral* properties);
    EdgePattern* newInEdge(const std::optional<Symbol>& symbol,
                           std::optional<std::vector<std::string_view>>&& types,
                           MapLiteral* properties);
    Projection* newProjection();
    MapLiteral* newMapLiteral();
    SinglePartQuery* newSinglePartQuery();
    NodePattern* newNode(const std::optional<Symbol>& symbol,
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
        requires std::is_base_of_v<SubStatement, T>
    T* newSubStatement(Args&&... args) {
        auto st = T::create(std::forward<Args>(args)...);
        auto* ptr = st.get();
        _subStatements.emplace_back(std::move(st));

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

    const std::vector<std::unique_ptr<QueryCommand>>& queries() const {
        return _queries;
    }

private:
    std::vector<std::unique_ptr<Expression>> _expressions;
    std::vector<std::unique_ptr<Pattern>> _patterns;
    std::vector<std::unique_ptr<PatternElement>> _patternElems;
    std::vector<std::unique_ptr<PatternEntity>> _patternEntity;
    std::vector<std::unique_ptr<Statement>> _statements;
    std::vector<std::unique_ptr<SubStatement>> _subStatements;
    std::vector<std::unique_ptr<Projection>> _projections;
    std::vector<std::unique_ptr<QueryCommand>> _queries;
    std::vector<std::unique_ptr<MapLiteral>> _mapLiterals;
    std::vector<std::unique_ptr<StatementContainer>> _statementContainers;

    StatementContainer* _currentStatements = nullptr;

    StatementContainer* newStatementContainer();
};

}
