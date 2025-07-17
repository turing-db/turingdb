#pragma once

#include <memory>
#include <optional>

#include "Projection.h"
#include "SinglePartQuery.h"
#include "expressions/AtomExpression.h"
#include "expressions/NodeLabelExpression.h"
#include "pattern/Pattern.h"
#include "pattern/PatternNode.h"
#include "pattern/PatternEdge.h"
#include "statements/ReadingStatementContainer.h"
#include "statements/Statement.h"
#include "Scope.h"
#include "Symbol.h"
#include "expressions/Expression.h"
#include "Literal.h"

namespace db {

class CypherAST {
public:
    CypherAST() {
        _rootScope = std::make_unique<Scope>();
        _currentScope = _rootScope.get();
    }

    void beginScope() {
        _currentScope = _currentScope->newInnerScope();
    }

    void endScope() {
        _currentScope = _currentScope->getParentScope();
    }

    Scope& currentScope() {
        return *_currentScope;
    }

    template <typename T, typename... Args>
        requires std::is_base_of_v<Statement, T>
    T* newStatement(Args&&... args) {
        auto st = T::create(std::forward<Args>(args)...);
        auto* ptr = st.get();
        _statements.emplace_back(std::move(st));

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

    PatternNode* nodeFromExpression(Expression* e) {
        if (e == nullptr) {
            return nullptr;
        }

        if (auto* atomExpr = dynamic_cast<AtomExpression*>(e)) {
            if (auto* value = std::get_if<Symbol>(&atomExpr->value())) {
                return newNode(*value, std::nullopt, nullptr);
            }

            if (auto* literal = std::get_if<Literal>(&atomExpr->value())) {
                if (auto* maplit = literal->as<MapLiteral*>()) {
                    return newNode(std::nullopt, std::nullopt, *maplit);
                }
            }
        }

        else if (const auto* nodeLabelExpr = dynamic_cast<db::NodeLabelExpression*>(e)) {
            return newNode(nodeLabelExpr->symbol(),
                           std::vector<std::string>(nodeLabelExpr->labels()),
                           nullptr);
        }

        return nullptr;
    }

    Pattern* newPattern() {
        auto& pattern = _patterns.emplace_back(Pattern::create());
        return pattern.get();
    }

    PatternPart* newPatternPart() {
        auto& patternPart = _patternParts.emplace_back(PatternPart::create());
        return patternPart.get();
    }

    PatternEdge* newOutEdge() {
        auto edge = PatternEdge::create();
        auto* ptr = edge.get();
        _patternEntity.emplace_back(std::move(edge));

        return ptr;
    }

    PatternEdge* newInEdge() {
        auto edge = PatternEdge::create();
        auto* ptr = edge.get();
        _patternEntity.emplace_back(std::move(edge));

        return ptr;
    }

    PatternNode* newNode(std::optional<Symbol>&& symbol,
                         std::optional<std::vector<std::string>>&& labels,
                         MapLiteral* properties) {
        auto node = PatternNode::create();

        node->setSymbol(std::move(symbol));
        node->setLabels(std::move(labels));
        node->setProperties(properties);

        auto* ptr = node.get();
        _patternEntity.emplace_back(std::move(node));

        return ptr;
    }

    ReadingStatementContainer* newReadingStatementContainer() {
        auto& readingStatements = _readingStatements.emplace_back(ReadingStatementContainer::create());
        return readingStatements.get();
    }

    Projection* newProjection() {
        auto& projection = _projections.emplace_back(Projection::create());
        return projection.get();
    }

    MapLiteral* newMapLiteral() {
        auto& mapLiteral = _mapLiterals.emplace_back(MapLiteral::create());
        return mapLiteral.get();
    }

    SinglePartQuery* newSinglePartQuery() {
        auto q = SinglePartQuery::create();
        auto* ptr = q.get();
        _queries.emplace_back(std::move(q));
        return ptr;
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
    std::vector<std::unique_ptr<ReadingStatementContainer>> _readingStatements;
    std::vector<std::unique_ptr<Projection>> _projections;
    std::vector<std::unique_ptr<Query>> _queries;
    std::vector<std::unique_ptr<MapLiteral>> _mapLiterals;

    Scope* _currentScope = nullptr;
};

}
