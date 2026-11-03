#include "MapProjectionExpr.h"

#include <algorithm>
#include <iterator>
#include <string_view>

#include "CypherAST.h"
#include "Symbol.h"

using namespace db;

MapProjectionExpr::MapProjectionExpr(SymbolExpr* base, const Items& items)
    : Expr(Expr::Kind::MAP_PROJECTION),
    _base(base),
    _items(items)
{
    for (auto item = _items.begin(); item != _items.end(); ++item) {
        if (!item->_key) {
            continue;
        }

        const std::string_view key = item->_key->getName();
        item->_shadowed = std::any_of(std::next(item), _items.end(), [key](const Item& later) {
            return later._key && later._key->getName() == key;
        });
    }
}

MapProjectionExpr::~MapProjectionExpr() {
}

MapProjectionExpr* MapProjectionExpr::create(CypherAST* ast, SymbolExpr* base, const Items& items) {
    MapProjectionExpr* expr = new MapProjectionExpr(base, items);
    ast->addExpr(expr);

    return expr;
}
