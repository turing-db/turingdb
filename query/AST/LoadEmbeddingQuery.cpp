#include "LoadEmbeddingQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

LoadEmbeddingQuery::LoadEmbeddingQuery(DeclContext* declContext,
                                       std::string_view filePath,
                                       std::string_view propertyName)
    : QueryCommand(declContext),
    _filePath(filePath),
    _propertyName(propertyName)
{
}

LoadEmbeddingQuery::~LoadEmbeddingQuery() {
}

LoadEmbeddingQuery* LoadEmbeddingQuery::create(CypherAST* ast,
                                               std::string_view filePath,
                                               std::string_view propertyName) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    LoadEmbeddingQuery* query = new LoadEmbeddingQuery(declContext, filePath, propertyName);
    ast->addQuery(query);
    return query;
}
