#include "DeleteVectorIndexQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

DeleteVectorIndexQuery::DeleteVectorIndexQuery(DeclContext* declContext,
                                               std::string_view indexName)
    : QueryCommand(declContext),
    _indexName(indexName)
{
}

DeleteVectorIndexQuery::~DeleteVectorIndexQuery() {
}

DeleteVectorIndexQuery* DeleteVectorIndexQuery::create(CypherAST* ast,
                                                       std::string_view indexName) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    DeleteVectorIndexQuery* query = new DeleteVectorIndexQuery(declContext, indexName);
    ast->addQuery(query);
    return query;
}
