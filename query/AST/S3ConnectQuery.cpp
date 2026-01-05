#include "S3ConnectQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

S3ConnectQuery::S3ConnectQuery(DeclContext* declContext,
                               std::string_view accessId,
                               std::string_view secretKey,
                               std::string_view region)
    : QueryCommand(declContext),
    _accessId(accessId),
    _secretKey(secretKey),
    _region(region)
{
}

S3ConnectQuery::~S3ConnectQuery() {
}

S3ConnectQuery* S3ConnectQuery::create(CypherAST* ast,
                                       std::string_view accessId,
                                       std::string_view secretKey,
                                       std::string_view region) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    S3ConnectQuery* query = new S3ConnectQuery(declContext, accessId, secretKey, region);
    ast->addQuery(query);
    return query;
}
