#include "CompoundStmt.h"

#include "CypherAST.h"

using namespace db::v2;

CompoundStmt::CompoundStmt() {
}

CompoundStmt::~CompoundStmt() {
}

CompoundStmt* CompoundStmt::create(CypherAST* ast) {
    CompoundStmt* stmt = new CompoundStmt();
    ast->addStmt(stmt);
    return stmt;
}
