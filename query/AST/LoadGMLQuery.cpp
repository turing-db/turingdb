#include "LoadGMLQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

LoadGMLQuery::LoadGMLQuery(DeclContext* declContext,
                           fs::Path&& filePath)
    : QueryCommand(declContext),
    _filePath(std::move(filePath))
{
}

LoadGMLQuery::~LoadGMLQuery() {
}

LoadGMLQuery* LoadGMLQuery::create(CypherAST* ast,
                                   fs::Path&& filePath) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    LoadGMLQuery* loadGML = new LoadGMLQuery(declContext, std::move(filePath));
    ast->addQuery(loadGML);
    return loadGML;
}
