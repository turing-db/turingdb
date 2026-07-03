#include "LoadParquetQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

LoadParquetQuery::LoadParquetQuery(DeclContext* declContext,
                                   fs::Path&& filePath)
    : QueryCommand(declContext),
    _filePath(std::move(filePath))
{
}

LoadParquetQuery::~LoadParquetQuery() {
}

LoadParquetQuery* LoadParquetQuery::create(CypherAST* ast,
                                           fs::Path&& filePath) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    LoadParquetQuery* loadParquet = new LoadParquetQuery(declContext, std::move(filePath));
    ast->addQuery(loadParquet);
    return loadParquet;
}
