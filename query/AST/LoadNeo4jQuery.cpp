#include "LoadNeo4jQuery.h"

#include "CypherAST.h"
#include "Path.h"
#include "decl/DeclContext.h"

using namespace db::v2;

LoadNeo4jQuery::LoadNeo4jQuery(DeclContext* declContext, fs::Path&& path)
    : QueryCommand(declContext),
    _path(std::move(path))
{
}

LoadNeo4jQuery::~LoadNeo4jQuery() {
}

LoadNeo4jQuery* LoadNeo4jQuery::create(CypherAST* ast,
                                       fs::Path&& path) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    LoadNeo4jQuery* q = new LoadNeo4jQuery(declContext, std::move(path));
    ast->addQuery(q);
    return q;
}
