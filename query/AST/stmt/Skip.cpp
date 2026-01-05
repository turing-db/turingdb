#include "Skip.h"

#include "CypherAST.h"

using namespace db::v2;

Skip::Skip(Expr* expr)
    : _expr(expr)
{
}

Skip::~Skip()
{
}

Skip* Skip::create(CypherAST* ast, Expr* expr) {
    Skip* stmt = new Skip(expr);
    ast->addSubStmt(stmt);
    return stmt;
}
