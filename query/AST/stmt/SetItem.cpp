#include "SetItem.h"
#include "expr/Expr.h"

#include "CypherAST.h"

namespace db::v2 {

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

SetItem* SetItem::create(CypherAST* ast, Symbol* symbol, Expr* value) {
    auto* setItem = new SetItem();
    setItem->_item.emplace<SymbolAddAssign>(SymbolAddAssign {symbol, value});
    ast->addSetItem(setItem);

    return setItem;
}

SetItem::SetItem()
{
}

SetItem::~SetItem() {
}

}
