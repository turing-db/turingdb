#include "NextSegmentStmt.h"

#include "CypherAST.h"

using namespace db;

NextSegmentStmt::NextSegmentStmt(CallSubqueryStmt* body)
    : _body(body)
{
}

NextSegmentStmt::~NextSegmentStmt() {
}

NextSegmentStmt* NextSegmentStmt::create(CypherAST* ast, CallSubqueryStmt* body) {
    NextSegmentStmt* stmt = new NextSegmentStmt(body);
    ast->addStmt(stmt);
    return stmt;
}
