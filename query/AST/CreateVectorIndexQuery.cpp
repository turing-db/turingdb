#include "CreateVectorIndexQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

CreateVectorIndexQuery::CreateVectorIndexQuery(DeclContext* declContext,
                                               std::string_view indexName,
                                               vec::Dimension dimension,
                                               vec::DistanceMetric metric,
                                               vec::IndexType indexType)
    : QueryCommand(declContext),
    _indexName(indexName),
    _dimension(dimension),
    _metric(metric),
    _indexType(indexType)
{
}

CreateVectorIndexQuery::~CreateVectorIndexQuery() {
}

CreateVectorIndexQuery* CreateVectorIndexQuery::create(CypherAST* ast,
                                                       std::string_view indexName,
                                                       vec::Dimension dimension,
                                                       vec::DistanceMetric metric,
                                                       vec::IndexType indexType) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    CreateVectorIndexQuery* query = new CreateVectorIndexQuery(declContext,
                                                               indexName,
                                                               dimension,
                                                               metric,
                                                               indexType);
    ast->addQuery(query);
    return query;
}
