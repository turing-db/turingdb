#include "OrderBy.h"

#include "CypherAST.h"

using namespace db::v2;

OrderBy::OrderBy()
{
}

OrderBy::~OrderBy()
{
}

OrderBy* OrderBy::create(CypherAST* ast) {
    OrderBy* stmt = new OrderBy;
    ast->addSubStmt(stmt);
    return stmt;
}
