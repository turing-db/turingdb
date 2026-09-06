#include "MergeStmt.h"

#include "CypherAST.h"

using namespace db;

MergeStmt::~MergeStmt() {
}

MergeStmt* MergeStmt::create(CypherAST* ast, Pattern* pattern) {
    MergeStmt* stmt = new MergeStmt(pattern);
    ast->addStmt(stmt);
    return stmt;
}
