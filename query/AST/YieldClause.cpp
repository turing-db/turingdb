#include "YieldClause.h"

#include "CypherAST.h"

using namespace db;

YieldClause* YieldClause::create(CypherAST* ast) {
    YieldClause* clause = new YieldClause();
    ast->addYieldClause(clause);
    return clause;
}
