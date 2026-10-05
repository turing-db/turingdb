#include "ConditionalQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

ConditionalQuery::ConditionalQuery(DeclContext* declContext, CallSubqueryStmt* body)
    : QueryCommand(declContext),
    _body(body)
{
}

ConditionalQuery::~ConditionalQuery() {
}

ConditionalQuery* ConditionalQuery::create(CypherAST* ast, CallSubqueryStmt* body) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    ConditionalQuery* query = new ConditionalQuery(declContext, body);

    ast->addQuery(query);

    return query;
}
