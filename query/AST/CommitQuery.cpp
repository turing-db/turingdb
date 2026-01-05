#include "CommitQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

CommitQuery::CommitQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

CommitQuery::~CommitQuery() {
}

CommitQuery* CommitQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    CommitQuery* query = new CommitQuery(declContext);
    ast->addQuery(query);
    return query;
}
