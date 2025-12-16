#pragma once

#include "QueryCommand.h"
#include <string_view>

namespace db::v2 {

class CypherAST;
class DeclContext;

class S3ConnectQuery : public QueryCommand {
public:
    static S3ConnectQuery* create(CypherAST* ast,
                                  std::string_view accessId,
                                  std::string_view secretKey,
                                  std::string_view region);

    Kind getKind() const override { return Kind::S3_CONNECT_QUERY; }

    std::string_view getAccessId() const { return _accessId; }
    std::string_view getSecretKey() const { return _secretKey; }
    std::string_view getRegion() const { return _region; }

private:
    std::string_view _accessId;
    std::string_view _secretKey;
    std::string_view _region;

    S3ConnectQuery(DeclContext* declContext,
                   std::string_view accessId,
                   std::string_view secretKey,
                   std::string_view region);
    ~S3ConnectQuery() override;
};

}
