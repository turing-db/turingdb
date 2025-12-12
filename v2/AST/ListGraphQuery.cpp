#include "ListGraphQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

ListGraphQuery::ListGraphQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

ListGraphQuery::~ListGraphQuery() {
}

ListGraphQuery* ListGraphQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    ListGraphQuery* query = new ListGraphQuery(declContext);
    ast->addQuery(query);
    return query;
}
