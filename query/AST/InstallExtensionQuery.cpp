#include "InstallExtensionQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

InstallExtensionQuery::InstallExtensionQuery(DeclContext* declContext,
                                             std::string_view name)
    : QueryCommand(declContext),
    _extensionName(name)
{
}

InstallExtensionQuery::~InstallExtensionQuery() {
}

InstallExtensionQuery* InstallExtensionQuery::create(CypherAST* ast,
                                                     std::string_view name) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    InstallExtensionQuery* query = new InstallExtensionQuery(declContext, name);
    ast->addQuery(query);
    return query;
}
