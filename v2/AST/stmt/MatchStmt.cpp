#include "MatchStmt.h"

#include "CypherAST.h"

using namespace db::v2;

MatchStmt::~MatchStmt() {
}

MatchStmt* MatchStmt::create(CypherAST* ast,
                             Pattern* pattern,
                             Skip* skip,
                             Limit* limit,
                             bool optional) {
    MatchStmt* stmt = new MatchStmt(pattern, skip, limit, optional);
    ast->addStmt(stmt);
    return stmt;
}
