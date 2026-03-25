#include "CreateNodePropertyIndexQuery.h"

#include <string_view>

#include "decl/DeclContext.h"
#include "CypherAST.h"
#include "QueryCommand.h"

using namespace db;

CreateNodePropertyIndexQuery::CreateNodePropertyIndexQuery(DeclContext* declCtxt,
                                                   std::string_view indexName,
                                                   NodePattern* node,
                                                   PropertyExpr* property)
    : QueryCommand(declCtxt),
    _indexName(indexName),
    _nodePattern(node),
    _propertyExpr(property)
{
}

CreateNodePropertyIndexQuery* CreateNodePropertyIndexQuery::create(CypherAST* ast,
                                                           std::string_view indexName,
                                                           NodePattern* node,
                                                           PropertyExpr* property) {
    constexpr DeclContext* parent = nullptr;
    DeclContext* declCtxt = DeclContext::create(ast, parent);

    CreateNodePropertyIndexQuery* query =
        new CreateNodePropertyIndexQuery(declCtxt, indexName, node, property);

    ast->addQuery(query);

    return query;
}
