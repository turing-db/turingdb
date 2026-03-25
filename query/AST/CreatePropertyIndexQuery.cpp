#include "CreatePropertyIndexQuery.h"

#include <string_view>

#include "decl/DeclContext.h"
#include "CypherAST.h"
#include "QueryCommand.h"

using namespace db;

CreatePropertyIndexQuery::CreatePropertyIndexQuery(DeclContext* declCtxt,
                                                   std::string_view propertyName)
    : QueryCommand(declCtxt),
    _propName(propertyName)
{
}

CreatePropertyIndexQuery* CreatePropertyIndexQuery::create(CypherAST* ast, std::string_view propertyName) {
    constexpr DeclContext* parent = nullptr;
    DeclContext* declCtxt = DeclContext::create(ast, parent);

    CreatePropertyIndexQuery* query =
        new CreatePropertyIndexQuery(declCtxt, propertyName);

    ast->addQuery(query);

    return query;
}
