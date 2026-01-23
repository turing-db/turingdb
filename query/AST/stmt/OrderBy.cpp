#include "OrderBy.h"

#include "CypherAST.h"

using namespace db;

OrderBy::OrderBy()
{
}

OrderBy::~OrderBy() {
}

OrderBy* OrderBy::create(CypherAST* ast) {
    OrderBy* stmt = new OrderBy;
    ast->addSubStmt(stmt);
    return stmt;
}
