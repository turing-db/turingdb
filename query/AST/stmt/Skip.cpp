#include "Skip.h"

#include "CypherAST.h"

using namespace db;

Skip::Skip(Expr* expr)
    : _expr(expr)
{
}

Skip::~Skip() {
}

Skip* Skip::create(CypherAST* ast, Expr* expr) {
    Skip* stmt = new Skip(expr);
    ast->addSubStmt(stmt);
    return stmt;
}
