#include "CreateVectorIndexQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

CreateVectorIndexQuery::CreateVectorIndexQuery(DeclContext* declContext,
                                               std::string_view indexName,
                                               vec::Dimension dimension,
                                               vec::DistanceMetric metric)
    : QueryCommand(declContext),
    _indexName(indexName),
    _dimension(dimension),
    _metric(metric)
{
}

CreateVectorIndexQuery::~CreateVectorIndexQuery() {
}

CreateVectorIndexQuery* CreateVectorIndexQuery::create(CypherAST* ast,
                                                       std::string_view indexName,
                                                       vec::Dimension dimension,
                                                       vec::DistanceMetric metric) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    CreateVectorIndexQuery* query = new CreateVectorIndexQuery(declContext,
                                                               indexName,
                                                               dimension,
                                                               metric);
    ast->addQuery(query);
    return query;
}
