#include "ListAvailableGraphsQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

ListAvailableGraphsQuery::ListAvailableGraphsQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

ListAvailableGraphsQuery::~ListAvailableGraphsQuery() {
}

ListAvailableGraphsQuery* ListAvailableGraphsQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    ListAvailableGraphsQuery* query = new ListAvailableGraphsQuery(declContext);
    ast->addQuery(query);
    return query;
}
