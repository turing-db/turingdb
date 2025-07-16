#pragma once

#include <memory>
#include <optional>

#include "pattern/Pattern.h"
#include "pattern/PatternNode.h"
#include "pattern/PatternEdge.h"
#include "Scope.h"
#include "Symbol.h"
#include "expressions/Expression.h"

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
        requires std::is_base_of_v<Expression, T>
    Expression* newExpression(Args&&... args) {
        auto& expr = _expressions.emplace_back(T::create(std::forward<Args>(args)...));
        return expr.get();
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
                         std::optional<std::vector<std::string>>&& labels) {
        auto node = PatternNode::create();
        auto* ptr = node.get();
        _patternEntity.emplace_back(std::move(node));

        return ptr;
    }

private:
    std::unique_ptr<Scope> _rootScope;

    std::vector<std::unique_ptr<Expression>> _expressions;
    std::vector<std::unique_ptr<Pattern>> _patterns;
    std::vector<std::unique_ptr<PatternPart>> _patternParts;
    std::vector<std::unique_ptr<PatternEntity>> _patternEntity;

    Scope* _currentScope = nullptr;
};

}
