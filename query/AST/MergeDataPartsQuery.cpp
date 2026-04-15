#include "MergeDataPartsQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

MergeDataPartsQuery::MergeDataPartsQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

MergeDataPartsQuery::~MergeDataPartsQuery() {
}

MergeDataPartsQuery* MergeDataPartsQuery::create(CypherAST* ast) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    MergeDataPartsQuery* query = new MergeDataPartsQuery(declContext);
    ast->addQuery(query);
    return query;
}
