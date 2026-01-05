#include "CreateStmt.h"

#include "CypherAST.h"

using namespace db;

CreateStmt::~CreateStmt() {
}

CreateStmt* CreateStmt::create(CypherAST* ast,
                             Pattern* pattern) {
    CreateStmt* stmt = new CreateStmt(pattern);
    ast->addStmt(stmt);
    return stmt;
}
