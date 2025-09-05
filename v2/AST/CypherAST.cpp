#include "CypherAST.h"

#include "expressions/SymbolExpression.h"
#include "expressions/LiteralExpression.h"
#include "expressions/NodeLabelExpression.h"
#include "types/Projection.h"
#include "types/SinglePartQuery.h"
#include "types/Pattern.h"
#include "types/NodePattern.h"
#include "types/EdgePattern.h"
#include "types/Literal.h"
#include "types/Symbol.h"
#include "statements/Statement.h"

using namespace db::v2;

CypherAST::CypherAST(std::string_view query)
    : _query(query),
      _currentStatements(newStatementContainer())
{
}

CypherAST::~CypherAST() = default;

NodePattern* CypherAST::nodeFromExpression(Expression* e) {
    if (e == nullptr) {
        return nullptr;
    }

    if (auto* symbolExpr = dynamic_cast<SymbolExpression*>(e)) {
        return newNode(symbolExpr->symbol(), std::nullopt, nullptr);
    }

    if (const auto* literalExpr = dynamic_cast<LiteralExpression*>(e)) {
        if (const auto* maplit = literalExpr->literal().as<MapLiteral*>()) {
            return newNode(std::nullopt, std::nullopt, *maplit);
        }
    }

    else if (const auto* nodeLabelExpr = dynamic_cast<NodeLabelExpression*>(e)) {
        return newNode(nodeLabelExpr->symbol(),
                       std::vector<std::string_view>(nodeLabelExpr->labelNames()),
                       nullptr);
    }

    return nullptr;
}

Pattern* CypherAST::newPattern() {
    auto& pattern = _patterns.emplace_back(Pattern::create());
    return pattern.get();
}

PatternElement* CypherAST::newPatternElem() {
    auto& patternPart = _patternElems.emplace_back(PatternElement::create());
    return patternPart.get();
}

EdgePattern* CypherAST::newOutEdge(const std::optional<Symbol>& symbol,
                                   std::optional<std::vector<std::string_view>>&& types,
                                   MapLiteral* properties) {
    auto edge = EdgePattern::create();

    edge->setSymbol(symbol);
    edge->setTypes(std::move(types));
    edge->setProperties(properties);

    auto* ptr = edge.get();
    _patternEntity.emplace_back(std::move(edge));

    return ptr;
}

EdgePattern* CypherAST::newInEdge(const std::optional<Symbol>& symbol,
                                  std::optional<std::vector<std::string_view>>&& types,
                                  MapLiteral* properties) {
    auto edge = EdgePattern::create();

    edge->setSymbol(symbol);
    edge->setTypes(std::move(types));
    edge->setProperties(properties);

    auto* ptr = edge.get();
    _patternEntity.emplace_back(std::move(edge));

    return ptr;
}

NodePattern* CypherAST::newNode(const std::optional<Symbol>& symbol,
                                std::optional<std::vector<std::string_view>>&& labels,
                                MapLiteral* properties) {
    auto node = NodePattern::create();

    node->setSymbol(symbol);
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
    auto q = SinglePartQuery::create(_declContainer, _currentStatements);
    auto* ptr = q.get();
    _queries.emplace_back(std::move(q));

    newStatementContainer();

    return ptr;
}

StatementContainer* CypherAST::newStatementContainer() {
    auto& statementContainer = _statementContainers.emplace_back(StatementContainer::create());
    _currentStatements = statementContainer.get();
    return statementContainer.get();
}
