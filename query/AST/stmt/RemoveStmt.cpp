#include "RemoveStmt.h"

#include "CypherAST.h"

using namespace db;

RemoveStmt::~RemoveStmt() {
}

RemoveStmt* RemoveStmt::create(CypherAST* ast) {
    RemoveStmt* stmt = new RemoveStmt();
    ast->addStmt(stmt);
    return stmt;
}
