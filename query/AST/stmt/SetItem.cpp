#include "SetItem.h"

#include "expr/Expr.h"

#include "CypherAST.h"

namespace db {

SetItem* SetItem::create(CypherAST* ast, PropertyExpr* expr, Expr* value) {
    auto* setItem = new SetItem();
    setItem->_item.emplace<PropertyExprAssign>(PropertyExprAssign {expr, value});
    ast->addSetItem(setItem);

    return setItem;
}

SetItem* SetItem::create(CypherAST* ast, EntityTypeExpr* value) {
    auto* setItem = new SetItem();
    setItem->_item.emplace<SymbolEntityTypes>(SymbolEntityTypes {value});
    ast->addSetItem(setItem);

    return setItem;
}

SetItem* SetItem::create(CypherAST* ast, Symbol* symbol, Expr* value, bool replaces) {
    auto* setItem = new SetItem();
    setItem->_item.emplace<SymbolMapAssign>(SymbolMapAssign {._symbol=symbol, ._value=value, ._replaces=replaces});
    ast->addSetItem(setItem);

    return setItem;
}

SetItem::SetItem()
{
}

SetItem::~SetItem() {
}

}
