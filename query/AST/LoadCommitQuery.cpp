#include "LoadCommitQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

LoadCommitQuery::LoadCommitQuery(DeclContext* declContext, std::string_view hashStr)
    : QueryCommand(declContext),
    _hashStr(hashStr)
{
}

LoadCommitQuery::~LoadCommitQuery() {
}

LoadCommitQuery* LoadCommitQuery::create(CypherAST* ast, std::string_view hashStr) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);

    LoadCommitQuery* loadCommit = new LoadCommitQuery(declContext, hashStr);
    ast->addQuery(loadCommit);

    return loadCommit;
}
