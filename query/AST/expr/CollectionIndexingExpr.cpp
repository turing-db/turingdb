#include "CollectionIndexingExpr.h"

#include "CypherAST.h"
#include "Literal.h"

using namespace db;

CollectionIndexingExpr::~CollectionIndexingExpr() {
}

CollectionIndexingExpr* CollectionIndexingExpr::create(CypherAST* ast) {
    CollectionIndexingExpr* expr = new CollectionIndexingExpr();
    ast->addExpr(expr);

    return expr;
}
