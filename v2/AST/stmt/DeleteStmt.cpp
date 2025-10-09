#include "DeleteStmt.h"

#include "CypherAST.h"

using namespace db::v2;

DeleteStmt::~DeleteStmt() {
}

DeleteStmt* DeleteStmt::create(CypherAST* ast,
                               ExprChain* exprChain) {
    DeleteStmt* stmt = new DeleteStmt(exprChain);
    ast->addStmt(stmt);
    return stmt;
}
