#include "MapProjectionExpr.h"

#include "CypherAST.h"

using namespace db;

MapProjectionExpr::MapProjectionExpr(SymbolExpr* base, const Items& items)
    : Expr(Expr::Kind::MAP_PROJECTION),
    _base(base),
    _items(items)
{
}

MapProjectionExpr::~MapProjectionExpr() {
}

MapProjectionExpr* MapProjectionExpr::create(CypherAST* ast, SymbolExpr* base, const Items& items) {
    MapProjectionExpr* expr = new MapProjectionExpr(base, items);
    ast->addExpr(expr);

    return expr;
}
