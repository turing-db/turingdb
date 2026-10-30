#include "NodePattern.h"

#include "CypherAST.h"
#include "Literal.h"
#include "expr/SymbolExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/MapProjectionExpr.h"

using namespace db;

NodePattern::NodePattern()
{
}

NodePattern::~NodePattern() {
}

NodePattern* NodePattern::create(CypherAST* ast) {
    NodePattern* pattern = new NodePattern();
    ast->addEntityPattern(pattern);
    return pattern;
}

NodePattern* NodePattern::fromExpr(CypherAST* ast, Expr* expr) {
    if (expr == nullptr) {
        return nullptr;
    }

    if (const SymbolExpr* symbolExpr = dynamic_cast<SymbolExpr*>(expr)) {
        NodePattern* pattern = NodePattern::create(ast);
        pattern->setSymbol(symbolExpr->getSymbol());
        return pattern;
    } else if (const LiteralExpr* literalExpr = dynamic_cast<LiteralExpr*>(expr)) {
        if (MapLiteral* maplit = dynamic_cast<MapLiteral*>(literalExpr->getLiteral())) {
            NodePattern* pattern = NodePattern::create(ast);
            pattern->setProperties(maplit);
            return pattern;
        }
    } else if (const MapProjectionExpr* projection = dynamic_cast<MapProjectionExpr*>(expr)) {
        MapLiteral* properties = createProperties(ast, projection->getItems());
        if (!properties) {
            return nullptr;
        }

        NodePattern* pattern = NodePattern::create(ast);
        pattern->setSymbol(projection->getBase()->getSymbol());
        pattern->setProperties(properties);
        return pattern;
    } else if (const EntityTypeExpr* nodeLabelExpr = dynamic_cast<EntityTypeExpr*>(expr)) {
        if (nodeLabelExpr->getOperand()) {
            return nullptr;
        }

        NodePattern* pattern = NodePattern::create(ast);
        pattern->setSymbol(nodeLabelExpr->getSymbol());
        pattern->setLabelExpr(nodeLabelExpr->getLabelExpr());
        return pattern;
    }

    return nullptr;
}

MapLiteral* NodePattern::createProperties(CypherAST* ast, const MapProjectionExpr::Items& items) {
    MapLiteral* properties = MapLiteral::create(ast);

    for (const MapProjectionExpr::Item& item : items) {
        if (item._kind != MapProjectionExpr::ItemKind::Literal) {
            return nullptr;
        }

        properties->set(item._key, item._value);
    }

    return properties;
}
