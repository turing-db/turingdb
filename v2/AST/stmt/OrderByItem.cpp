#include "OrderByItem.h"

#include "CypherAST.h"

using namespace db::v2;

OrderByItem* OrderByItem::create(CypherAST* ast, Expr* expr, OrderByType type) {
    OrderByItem* item = new OrderByItem(expr, type);
    ast->addOrderByItem(item);
    return item;
}
