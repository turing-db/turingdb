#include "ExprChain.h"

#include "CypherAST.h"

using namespace db::v2;

ExprChain::ExprChain()
{
}

ExprChain::~ExprChain() {
}

ExprChain* ExprChain::create(CypherAST* ast) {
    ExprChain* chain = new ExprChain();
    ast->addExprChain(chain);
    return chain;
}
