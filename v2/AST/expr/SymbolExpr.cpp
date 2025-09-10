#include "SymbolExpr.h"

#include "CypherAST.h"

using namespace db::v2;

SymbolExpr::~SymbolExpr() {
}

SymbolExpr* SymbolExpr::create(CypherAST* ast, const Symbol& symbol) {
    SymbolExpr* expr = new SymbolExpr(symbol);
    ast->addExpr(expr);
    return expr;
}
