#include "SetStmt.h"

#include "CypherAST.h"

using namespace db::v2;

SetStmt::~SetStmt() {
}

SetStmt* SetStmt::create(CypherAST* ast,
                             Pattern* pattern) {
    SetStmt* stmt = new SetStmt(pattern);
    ast->addStmt(stmt);
    return stmt;
}
