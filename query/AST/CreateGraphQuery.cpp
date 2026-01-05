#include "CreateGraphQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

CreateGraphQuery::CreateGraphQuery(DeclContext* declContext,
                                   std::string_view name)
    : QueryCommand(declContext),
    _graphName(name)
{
}

CreateGraphQuery::~CreateGraphQuery() {
}

CreateGraphQuery* CreateGraphQuery::create(CypherAST* ast,
                                           std::string_view name) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    CreateGraphQuery* query = new CreateGraphQuery(declContext, name);
    ast->addQuery(query);
    return query;
}
