#include "CallStmt.h"

#include "CypherAST.h"

using namespace db;

CallStmt::CallStmt()
{
}

CallStmt::~CallStmt() {
}

CallStmt* CallStmt::create(CypherAST* ast) {
    CallStmt* stmt = new CallStmt();
    ast->addStmt(stmt);

    return stmt;
}
