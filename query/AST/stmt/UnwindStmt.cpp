#include "UnwindStmt.h"

#include "CypherAST.h"

using namespace db;

UnwindStmt::~UnwindStmt() {
}

UnwindStmt* UnwindStmt::create(CypherAST* ast, Expr* expr, Symbol* sym) {
    UnwindStmt* stmt = new UnwindStmt(expr, sym);
    ast->addStmt(stmt);
    return stmt;
}
