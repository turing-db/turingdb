#include "NodeLabelExpr.h"

#include "CypherAST.h"

using namespace db::v2;

NodeLabelExpr::~NodeLabelExpr() {
}

NodeLabelExpr* NodeLabelExpr::create(CypherAST* ast, 
                                     const Symbol& symbol,
                                     LabelVector&& labels) {
    NodeLabelExpr* expr = new NodeLabelExpr(symbol, std::move(labels));
    ast->addExpr(expr);
    return expr;
}
