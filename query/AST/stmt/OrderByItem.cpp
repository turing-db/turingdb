#include "OrderByItem.h"

#include "CypherAST.h"

using namespace db;

OrderByItem* OrderByItem::create(CypherAST* ast, Expr* expr, OrderByType type) {
    OrderByItem* item = new OrderByItem(expr, type);
    ast->addOrderByItem(item);
    return item;
}
