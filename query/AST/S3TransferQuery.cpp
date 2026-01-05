#include "S3TransferQuery.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

S3TransferQuery::S3TransferQuery(DeclContext* declContext,
                                 Direction direction,
                                 std::string_view s3Url,
                                 std::string_view localPath)
    : QueryCommand(declContext),
    _direction(direction),
    _s3Url(s3Url),
    _localPath(localPath)
{
}

S3TransferQuery::~S3TransferQuery() {
}

S3TransferQuery* S3TransferQuery::create(CypherAST* ast,
                                         Direction direction,
                                         std::string_view s3Url,
                                         std::string_view localPath) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    S3TransferQuery* query = new S3TransferQuery(declContext,
                                                 direction,
                                                 s3Url,
                                                 localPath);
    ast->addQuery(query);
    return query;
}
