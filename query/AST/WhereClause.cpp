#include "WhereClause.h"

#include "CypherAST.h"

using namespace db::v2;

WhereClause* WhereClause::create(CypherAST* ast, Expr* expr) {
    WhereClause* clause = new WhereClause(expr);
    ast->addWhereClause(clause);
    return clause;
}
