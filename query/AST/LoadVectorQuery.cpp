#include "LoadVectorQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

LoadVectorQuery::LoadVectorQuery(DeclContext* declContext,
                                 std::string_view filePath,
                                 std::string_view indexName)
    : QueryCommand(declContext),
    _filePath(filePath),
    _indexName(indexName)
{
}

LoadVectorQuery::~LoadVectorQuery() {
}

LoadVectorQuery* LoadVectorQuery::create(CypherAST* ast,
                                         std::string_view filePath,
                                         std::string_view indexName) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    LoadVectorQuery* query = new LoadVectorQuery(declContext, filePath, indexName);
    ast->addQuery(query);
    return query;
}
