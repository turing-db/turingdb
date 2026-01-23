#pragma once

#include <string_view>
#include <stdint.h>

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class S3TransferQuery : public QueryCommand {
public:
    enum class Direction : uint8_t {
        PULL = 0,
        PUSH
    };

    static S3TransferQuery* create(CypherAST* ast,
                                   Direction direction,
                                   std::string_view s3Url,
                                   std::string_view localPath);

    Kind getKind() const override { return Kind::S3_TRANSFER_QUERY; }

    Direction getDirection() const { return _direction; }

    std::string_view getS3Url() const { return _s3Url; }
    std::string_view getLocalPath() const { return _localPath; }

    std::string_view getS3Bucket() const { return _s3Bucket; }
    std::string_view getS3Prefix() const { return _s3Prefix; }
    std::string_view getS3File() const { return _s3File; }

    void setS3Bucket(std::string_view bucket) { _s3Bucket = bucket; }
    void setS3Prefix(std::string_view prefix) { _s3Prefix = prefix; }
    void setS3File(std::string_view file) { _s3File = file; }

private:
    Direction _direction;

    std::string_view _s3Url;
    std::string_view _localPath;

    std::string_view _s3Bucket;
    std::string_view _s3Prefix;
    std::string_view _s3File;

    S3TransferQuery(DeclContext* declContext,
                    Direction direction,
                    std::string_view s3Url,
                    std::string_view localPath);

    ~S3TransferQuery() override;
};

}
