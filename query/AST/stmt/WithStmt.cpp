#include "WithStmt.h"

#include "CypherAST.h"

using namespace db;

WithStmt::WithStmt(Projection* projection)
    : _projection(projection)
{
}

WithStmt::~WithStmt() {
}

WithStmt* WithStmt::create(CypherAST* ast, Projection* projection) {
    WithStmt* stmt = new WithStmt(projection);
    ast->addStmt(stmt);
    return stmt;
}
