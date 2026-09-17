#include "CallSubqueryStmt.h"

#include "CypherAST.h"
#include "SinglePartQuery.h"

using namespace db;

CallSubqueryStmt::CallSubqueryStmt(SinglePartQuery* body)
    : _body(body)
{
}

CallSubqueryStmt::~CallSubqueryStmt() {
}

CallSubqueryStmt* CallSubqueryStmt::create(CypherAST* ast, SinglePartQuery* body) {
    ast->adoptSubquery(body);

    CallSubqueryStmt* stmt = new CallSubqueryStmt(body);
    ast->addStmt(stmt);

    return stmt;
}

bool CallSubqueryStmt::isReturning() const {
    return _body->getReturnStmt() != nullptr;
}
