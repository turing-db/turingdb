#include "CypherAST.h"

#include "expressions/AtomExpression.h"
#include "expressions/NodeLabelExpression.h"
#include "expressions/Expression.h"
#include "types/Projection.h"
#include "types/SinglePartQuery.h"
#include "types/Pattern.h"
#include "types/PatternNode.h"
#include "types/PatternEdge.h"
#include "types/Scope.h"
#include "types/Literal.h"
#include "types/Symbol.h"
#include "statements/Statement.h"

using namespace db;


CypherAST::CypherAST()
{
    _rootScope = std::make_unique<Scope>();
    _currentStatements = newStatementContainer();
    _currentScope = _rootScope.get();
}

CypherAST::~CypherAST() = default;

void CypherAST::beginScope() {
    _currentScope = _currentScope->newInnerScope();
}

void CypherAST::endScope() {
    _currentScope = _currentScope->getParentScope();
}

PatternNode* CypherAST::nodeFromExpression(Expression* e) {
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
                       std::vector<std::string_view>(nodeLabelExpr->labels()),
                       nullptr);
    }

    return nullptr;
}


Pattern* CypherAST::newPattern() {
    auto& pattern = _patterns.emplace_back(Pattern::create());
    return pattern.get();
}

PatternPart* CypherAST::newPatternPart() {
    auto& patternPart = _patternParts.emplace_back(PatternPart::create());
    return patternPart.get();
}

PatternEdge* CypherAST::newOutEdge() {
    auto edge = PatternEdge::create();
    auto* ptr = edge.get();
    _patternEntity.emplace_back(std::move(edge));

    return ptr;
}

PatternEdge* CypherAST::newInEdge() {
    auto edge = PatternEdge::create();
    auto* ptr = edge.get();
    _patternEntity.emplace_back(std::move(edge));

    return ptr;
}

PatternNode* CypherAST::newNode(std::optional<Symbol>&& symbol,
                                std::optional<std::vector<std::string_view>>&& labels,
                                MapLiteral* properties) {
    auto node = PatternNode::create();

    node->setSymbol(std::move(symbol));
    node->setLabels(std::move(labels));
    node->setProperties(properties);

    auto* ptr = node.get();
    _patternEntity.emplace_back(std::move(node));

    return ptr;
}

Projection* CypherAST::newProjection() {
    auto& projection = _projections.emplace_back(Projection::create());
    return projection.get();
}

MapLiteral* CypherAST::newMapLiteral() {
    auto& mapLiteral = _mapLiterals.emplace_back(MapLiteral::create());
    return mapLiteral.get();
}

SinglePartQuery* CypherAST::newSinglePartQuery() {
    auto q = SinglePartQuery::create(_currentStatements);
    auto* ptr = q.get();
    _queries.emplace_back(std::move(q));

    newStatementContainer();

    return ptr;
}

StatementContainer* CypherAST::newStatementContainer() {
    auto& statementContainer = _statementContainers.emplace_back(StatementContainer::create());
    return statementContainer.get();
}
