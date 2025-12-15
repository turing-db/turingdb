#include "LoadGMLQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

LoadGMLQuery::LoadGMLQuery(DeclContext* declContext,
                           std::string_view graphName,
                           std::string_view filePath)
    : QueryCommand(declContext),
    _graphName(graphName),
    _filePath(filePath)
{
}

LoadGMLQuery::~LoadGMLQuery() {
}

LoadGMLQuery* LoadGMLQuery::create(CypherAST* ast,
                                   std::string_view graphName,
                                   std::string_view filePath) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    LoadGMLQuery* loadGML = new LoadGMLQuery(declContext, graphName, filePath);
    ast->addQuery(loadGML);
    return loadGML;
}
