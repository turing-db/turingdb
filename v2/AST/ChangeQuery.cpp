#include "ChangeQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

ChangeQuery::ChangeQuery(DeclContext* declContext, ChangeOp op)
    : QueryCommand(declContext),
    _op(op)
{
}

ChangeQuery::~ChangeQuery() {
}

ChangeQuery* ChangeQuery::create(CypherAST* ast, ChangeOp op) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    ChangeQuery* query = new ChangeQuery(declContext, op);
    ast->addQuery(query);
    return query;
}
