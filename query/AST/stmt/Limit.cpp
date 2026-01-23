#include "Limit.h"

#include "CypherAST.h"

using namespace db;

Limit::Limit(Expr* expr)
    : _expr(expr)
{
}

Limit::~Limit() {
}

Limit* Limit::create(CypherAST* ast, Expr* expr) {
    Limit* stmt = new Limit(expr);
    ast->addSubStmt(stmt);
    return stmt;
}
