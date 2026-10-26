#include "CallSubqueryStmt.h"

#include "CypherAST.h"
#include "RegularQuery.h"

using namespace db;

CallSubqueryStmt::CallSubqueryStmt(RegularQuery* body)
    : _body(body)
{
}

CallSubqueryStmt::~CallSubqueryStmt() {
}

CallSubqueryStmt* CallSubqueryStmt::create(CypherAST* ast, RegularQuery* body) {
    ast->nestQuery(body);

    CallSubqueryStmt* stmt = new CallSubqueryStmt(body);
    ast->addStmt(stmt);

    return stmt;
}

bool CallSubqueryStmt::isReturning() const {
    return _body->isReturning();
}

bool CallSubqueryStmt::writesToTheGraph() const {
    return _body->writesToTheGraph();
}

bool CallSubqueryStmt::readsTheGraph() const {
    return _body->readsTheGraph();
}
