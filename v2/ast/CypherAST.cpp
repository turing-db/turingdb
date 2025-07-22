#include "CypherAST.h"

#include "expressions/AtomExpression.h"
#include "expressions/NodeLabelExpression.h"
#include "expressions/Expression.h"
#include "types/Projection.h"
#include "types/SinglePartQuery.h"
#include "types/Pattern.h"
#include "types/NodePattern.h"
#include "types/PatternEdge.h"
#include "types/Literal.h"
#include "types/Symbol.h"
#include "statements/Statement.h"

using namespace db;


CypherAST::CypherAST()
    : _currentStatements(newStatementContainer())
{
}

CypherAST::~CypherAST() = default;

NodePattern* CypherAST::nodeFromExpression(Expression* e) {
    if (e == nullptr) {
        return nullptr;
    }

    if (auto* atomExpr = dynamic_cast<AtomExpression*>(e)) {
        if (auto* value = std::get_if<Symbol>(&atomExpr->atom())) {
            return newNode(*value, std::nullopt, nullptr);
        }

        if (auto* literal = std::get_if<Literal>(&atomExpr->atom())) {
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
    auto q = SinglePartQuery::create(_currentStatements);
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
