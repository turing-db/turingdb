#include "LoadGraphQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

LoadGraphQuery::LoadGraphQuery(DeclContext* declContext, std::string_view graphName)
    : QueryCommand(declContext),
    _graphName(graphName)
{
}

LoadGraphQuery::~LoadGraphQuery() {
}

LoadGraphQuery* LoadGraphQuery::create(CypherAST* ast,
                                       std::string_view graphName) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    LoadGraphQuery* loadGraph = new LoadGraphQuery(declContext, graphName);
    ast->addQuery(loadGraph);
    return loadGraph;
}
