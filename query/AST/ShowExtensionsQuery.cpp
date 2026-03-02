#include "ShowExtensionsQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

ShowExtensionsQuery::ShowExtensionsQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

ShowExtensionsQuery::~ShowExtensionsQuery() {
}

ShowExtensionsQuery* ShowExtensionsQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    ShowExtensionsQuery* query = new ShowExtensionsQuery(declContext);
    ast->addQuery(query);
    return query;
}
