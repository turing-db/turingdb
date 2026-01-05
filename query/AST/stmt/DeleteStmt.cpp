#include "DeleteStmt.h"

#include "CypherAST.h"

using namespace db;

DeleteStmt::~DeleteStmt() {
}

DeleteStmt* DeleteStmt::create(CypherAST* ast,
                               ExprChain* exprChain) {
    DeleteStmt* stmt = new DeleteStmt(exprChain);
    ast->addStmt(stmt);
    return stmt;
}
