#include "NodePattern.h"

#include "CypherAST.h"
#include "expr/SymbolExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/NodeLabelExpr.h"
#include "expr/Literal.h"

using namespace db::v2;

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
    } else if (const NodeLabelExpr* nodeLabelExpr = dynamic_cast<NodeLabelExpr*>(expr)) {
        NodePattern* pattern = NodePattern::create(ast);
        pattern->setSymbol(nodeLabelExpr->getSymbol());
        pattern->setLabels(nodeLabelExpr->labels());
    }

    return nullptr;
}
