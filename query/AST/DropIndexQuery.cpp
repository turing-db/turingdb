#include "DropIndexQuery.h"
#include "QueryCommand.h"
#include "decl/DeclContext.h"

using namespace db;


DropIndexQuery::DropIndexQuery(DeclContext* declCtxt, std::string_view indexName)
    : QueryCommand(declCtxt),
    _indexName(indexName)
{
}

DropIndexQuery* DropIndexQuery::create(CypherAST *ast, std::string_view indexName) {
    constexpr DeclContext* parent = nullptr;
    DeclContext* declCtxt = DeclContext::create(ast, parent);

    DropIndexQuery* query = new DropIndexQuery(declCtxt, indexName);

    ast->addQuery(query);

    return query;
}
