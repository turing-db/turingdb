#include "LoadJsonlQuery.h"

#include "CypherAST.h"
#include "Path.h"
#include "decl/DeclContext.h"

using namespace db;

LoadJsonlQuery::LoadJsonlQuery(DeclContext* declContext, fs::Path&& path)
    : QueryCommand(declContext),
    _path(std::move(path))
{
}

LoadJsonlQuery::~LoadJsonlQuery() {
}

LoadJsonlQuery* LoadJsonlQuery::create(CypherAST* ast,
                                       fs::Path&& path) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    LoadJsonlQuery* q = new LoadJsonlQuery(declContext, std::move(path));
    ast->addQuery(q);
    return q;
}
