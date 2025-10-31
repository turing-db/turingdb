#include "YieldClause.h"

#include "CypherAST.h"

using namespace db::v2;

YieldClause* YieldClause::create(CypherAST* ast) {
    YieldClause* clause = new YieldClause();
    ast->addYieldClause(clause);
    return clause;
}
