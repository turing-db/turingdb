#include "NodeLabelExpr.h"

#include "CypherAST.h"

using namespace db::v2;

NodeLabelExpr::NodeLabelExpr(Symbol* symbol, Labels&& labels)
    : Expr(Kind::NODE_LABEL),
    _symbol(symbol),
    _labels(std::move(labels))
{
}

NodeLabelExpr::~NodeLabelExpr() {
}

NodeLabelExpr* NodeLabelExpr::create(CypherAST* ast, 
                                     Symbol* symbol,
                                     Labels&& labels) {
    NodeLabelExpr* expr = new NodeLabelExpr(symbol, std::move(labels));
    ast->addExpr(expr);
    return expr;
}
