#include "ShowVectorIndexesQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

ShowVectorIndexesQuery::ShowVectorIndexesQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

ShowVectorIndexesQuery::~ShowVectorIndexesQuery() {
}

ShowVectorIndexesQuery* ShowVectorIndexesQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    ShowVectorIndexesQuery* query = new ShowVectorIndexesQuery(declContext);
    ast->addQuery(query);
    return query;
}
