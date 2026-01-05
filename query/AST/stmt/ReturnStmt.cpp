#include "ReturnStmt.h"

#include "CypherAST.h"

using namespace db;

ReturnStmt::ReturnStmt(Projection* projection)
    : _projection(projection)
{
}

ReturnStmt::~ReturnStmt() {
}

ReturnStmt* ReturnStmt::create(CypherAST* ast, Projection* projection) {
    ReturnStmt* stmt = new ReturnStmt(projection);
    ast->addStmt(stmt);
    return stmt;
}
