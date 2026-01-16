#include "ShowProceduresQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

ShowProceduresQuery::ShowProceduresQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

ShowProceduresQuery::~ShowProceduresQuery() {
}

ShowProceduresQuery* ShowProceduresQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    ShowProceduresQuery* query = new ShowProceduresQuery(declContext);
    ast->addQuery(query);
    return query;
}
