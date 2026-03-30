#include "CreateEdgePropertyIndexQuery.h"

#include <string_view>

#include "decl/DeclContext.h"
#include "CypherAST.h"
#include "QueryCommand.h"

using namespace db;

CreateEdgePropertyIndexQuery::CreateEdgePropertyIndexQuery(DeclContext* declCtxt,
                                                           std::string_view indexName,
                                                           EdgePattern* edge,
                                                           PropertyExpr* property)
    : QueryCommand(declCtxt),
    _indexName(indexName),
    _edgePattern(edge),
    _propertyExpr(property)
{
}

CreateEdgePropertyIndexQuery* CreateEdgePropertyIndexQuery::create(CypherAST* ast,
                                                                   std::string_view indexName,
                                                                   EdgePattern* edge,
                                                                   PropertyExpr* property) {
    constexpr DeclContext* parent = nullptr;
    DeclContext* declCtxt = DeclContext::create(ast, parent);

    CreateEdgePropertyIndexQuery* query =
        new CreateEdgePropertyIndexQuery(declCtxt, indexName, edge, property);

    ast->addQuery(query);

    return query;
}
