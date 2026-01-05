#include "SetStmt.h"

#include "CypherAST.h"

using namespace db::v2;

SetStmt::~SetStmt() {
}

SetStmt* SetStmt::create(CypherAST* ast) {
    SetStmt* stmt = new SetStmt();
    ast->addStmt(stmt);
    return stmt;
}
