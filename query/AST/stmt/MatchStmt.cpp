#include "MatchStmt.h"

#include "CypherAST.h"

using namespace db::v2;

MatchStmt::~MatchStmt() {
}

MatchStmt* MatchStmt::create(CypherAST* ast,
                             Pattern* pattern) {
    MatchStmt* stmt = new MatchStmt(pattern);
    ast->addStmt(stmt);
    return stmt;
}
