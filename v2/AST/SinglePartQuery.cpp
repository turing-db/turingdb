#include "SinglePartQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

SinglePartQuery::SinglePartQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

SinglePartQuery::~SinglePartQuery() {
}

SinglePartQuery* SinglePartQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    SinglePartQuery* query = new SinglePartQuery(declContext);
    ast->addQuery(query);
    return query;
}
