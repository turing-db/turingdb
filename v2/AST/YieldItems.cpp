#include "YieldItems.h"

#include "CypherAST.h"

using namespace db::v2;

YieldItems::YieldItems()
{
}

YieldItems::~YieldItems() {
}

YieldItems* YieldItems::create(CypherAST* ast) {
    YieldItems* items = new YieldItems();
    ast->addYieldItems(items);
    return items;
}
